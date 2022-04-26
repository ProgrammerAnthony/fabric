/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"context"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	lb "github.com/hyperledger/fabric-protos-go/peer/lifecycle"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/core/chaincode/persistence"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Reader defines the interface needed for reading a file.
type Reader interface {
	ReadFile(string) ([]byte, error)
}

// Installer holds the dependencies needed to install
// a chaincode.
type Installer struct {
	Command        *cobra.Command
	EndorserClient EndorserClient
	Input          *InstallInput
	Reader         Reader
	Signer         Signer
}

// InstallInput holds the input parameters for installing
// a chaincode.
type InstallInput struct {
	PackageFile string
}

// Validate checks that the required install parameters
// are provided.
func (i *InstallInput) Validate() error {
	if i.PackageFile == "" {
		return errors.New("chaincode install package must be provided")
	}

	return nil
}

// InstallCmd returns the cobra command for chaincode install.
func InstallCmd(i *Installer, cryptoProvider bccsp.BCCSP) *cobra.Command {
	chaincodeInstallCmd := &cobra.Command{
		Use:       "install",
		Short:     "Install a chaincode.",
		Long:      "Install a chaincode on a peer.",
		ValidArgs: []string{"1"},
		RunE: func(cmd *cobra.Command, args []string) error {
			//若安装对象为空，则创建
			if i == nil {
				//整理连接背书节点的背书客户端的参数，主要有两个来源：命令行参数、通道peer节点连接配置文件（由--connectionProfile指定，这里未使用）
				ccInput := &ClientConnectionsInput{
					CommandName: cmd.Name(),
					//(成功安装后，链码安装包将存储于背书节点本地，可供查询和其他客户端拉取)
					EndorserRequired:      true,
					ChannelID:             channelID,
					PeerAddresses:         peerAddresses,
					TLSRootCertFiles:      tlsRootCertFiles,
					ConnectionProfilePath: connectionProfilePath,
					TLSEnabled:            viper.GetBool("peer.tls.enabled"),
				}
				//依据客户端参数，创建用于连接peer、orderer节点的各类客户端，这里是背书客户端。
				c, err := NewClientConnections(ccInput, cryptoProvider)
				if err != nil {
					return err
				}

				// install is currently only supported for one peer so just use
				// the first endorser client
				//使用背书客户端、文件系统读写对象、peer CLI身份，创建链码安装对象。
				i = &Installer{
					Command:        cmd,
					EndorserClient: c.EndorserClients[0],
					Reader:         &persistence.FilesystemIO{},
					Signer:         c.Signer,
				}
			}
			//使用链码安装对象，安装mycc.tar.gz。
			return i.InstallChaincode(args)
		},
	}
	flagList := []string{
		"peerAddresses",
		"tlsRootCertFiles",
		"connectionProfile",
	}
	attachFlags(chaincodeInstallCmd, flagList)

	return chaincodeInstallCmd
}

// InstallChaincode installs the chaincode.
//使用链码安装对象，安装mycc.tar.gz。
func (i *Installer) InstallChaincode(args []string) error {
	if i.Command != nil {
		// Parsing of the command line is done so silence cmd usage
		i.Command.SilenceUsage = true
	}

	i.setInput(args)

	return i.Install()
}

func (i *Installer) setInput(args []string) {
	i.Input = &InstallInput{}

	if len(args) > 0 {
		i.Input.PackageFile = args[0]
	}
}

// Install installs a chaincode for use with _lifecycle.
func (i *Installer) Install() error {
	//验证输入参数，使用文件系统读写对象，读取mycc.tar.gz。
	err := i.Input.Validate()
	if err != nil {
		return err
	}

	pkgBytes, err := i.Reader.ReadFile(i.Input.PackageFile)
	if err != nil {
		return errors.WithMessagef(err, "failed to read chaincode package at '%s'", i.Input.PackageFile)
	}

	//获取peer CLI的身份，代表发起安装链码背书申请的客户端。
	serializedSigner, err := i.Signer.Serialize()
	if err != nil {
		return errors.Wrap(err, "failed to serialize signer")
	}

	//创建背书申请，其中包含调用_lifecycle的链码调用说明书（CIS）、
	//调用链码时所用的参数数组（索引0处为调用的方法名，索引1处为mycc.tar.gz压缩包数据）。
	//该背书申请是一个无通道背书交易，即安装阶段，链码不存在通道归属的概念，已安装的链码可以根据需要实例化在任何通道中。
	proposal, err := i.createInstallProposal(pkgBytes, serializedSigner)
	if err != nil {
		return err
	}

	//使用peer CLI的身份对背书申请签名，以表明发起背书申请的客户端身份。
	signedProposal, err := signProposal(proposal, i.Signer)
	if err != nil {
		return errors.WithMessage(err, "failed to create signed proposal for chaincode install")
	}

	//使用客户端向背书节点发送安装链码的背书申请，并返回安装结果。
	return i.submitInstallProposal(signedProposal)
}

func (i *Installer) submitInstallProposal(signedProposal *pb.SignedProposal) error {
	proposalResponse, err := i.EndorserClient.ProcessProposal(context.Background(), signedProposal)
	if err != nil {
		return errors.WithMessage(err, "failed to endorse chaincode install")
	}

	if proposalResponse == nil {
		return errors.New("chaincode install failed: received nil proposal response")
	}

	if proposalResponse.Response == nil {
		return errors.New("chaincode install failed: received proposal response with nil response")
	}

	if proposalResponse.Response.Status != int32(cb.Status_SUCCESS) {
		return errors.Errorf("chaincode install failed with status: %d - %s", proposalResponse.Response.Status, proposalResponse.Response.Message)
	}
	logger.Infof("Installed remotely: %v", proposalResponse)

	icr := &lb.InstallChaincodeResult{}
	err = proto.Unmarshal(proposalResponse.Response.Payload, icr)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal proposal response's response payload")
	}
	logger.Infof("Chaincode code package identifier: %s", icr.PackageId)

	return nil
}

func (i *Installer) createInstallProposal(pkgBytes []byte, creatorBytes []byte) (*pb.Proposal, error) {
	installChaincodeArgs := &lb.InstallChaincodeArgs{
		ChaincodeInstallPackage: pkgBytes,
	}

	installChaincodeArgsBytes, err := proto.Marshal(installChaincodeArgs)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal InstallChaincodeArgs")
	}

	ccInput := &pb.ChaincodeInput{Args: [][]byte{[]byte("InstallChaincode"), installChaincodeArgsBytes}}

	cis := &pb.ChaincodeInvocationSpec{
		ChaincodeSpec: &pb.ChaincodeSpec{
			ChaincodeId: &pb.ChaincodeID{Name: lifecycleName},
			Input:       ccInput,
		},
	}

	proposal, _, err := protoutil.CreateProposalFromCIS(cb.HeaderType_ENDORSER_TRANSACTION, "", cis, creatorBytes)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create proposal for ChaincodeInvocationSpec")
	}

	return proposal, nil
}
