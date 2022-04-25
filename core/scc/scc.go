/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package scc

import (
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/container/ccintf"
)

// SysCCVersion is a constant used for the version field of system chaincodes.
// Historically, the version of a system chaincode was implicitly tied to the exact
// build version of a peer, which does not work for collecting endorsements across
// sccs across peers.  Until there is a need for separate SCC versions, we use
// this constant here.
const SysCCVersion = "syscc"

// ChaincodeID returns the chaincode ID of a system chaincode of a given name.
func ChaincodeID(ccName string) string {
	return ccName + "." + SysCCVersion
}

// XXX should we change this name to actually match the package name? Leaving now for historical consistency
var sysccLogger = flogging.MustGetLogger("sccapi")

// BuiltinSCCs are special system chaincodes, differentiated from other (plugin)
// system chaincodes.  These chaincodes do not need to be initialized in '_lifecycle'
// and may be invoked without a channel context.  It is expected that '_lifecycle'
// will eventually be the only builtin SCCs.
// Note, this field should only be used on _endorsement_ side, never in validation
// as it might change.
type BuiltinSCCs map[string]struct{}

func (bccs BuiltinSCCs) IsSysCC(name string) bool {
	_, ok := bccs[name]
	return ok
}

// A ChaincodeStreamHandler is responsible for handling the ChaincodeStream
// communication between a per and chaincode.
type ChaincodeStreamHandler interface {
	HandleChaincodeStream(ccintf.ChaincodeStream) error
	LaunchInProc(packageID string) <-chan struct{}
}

type SelfDescribingSysCC interface {
	//Unique name of the system chaincode
	Name() string

	// Chaincode returns the underlying chaincode
	Chaincode() shim.Chaincode
}

// DeploySysCC is the hook for system chaincodes where system chaincodes are registered with the fabric.
// This call directly registers the chaincode with the chaincode handler and bypasses the other usercc constructs.
func DeploySysCC(sysCC SelfDescribingSysCC, chaincodeStreamHandler ChaincodeStreamHandler) {
	sysccLogger.Infof("deploying system chaincode '%s'", sysCC.Name())

	//查看_lifecycle的启动状态，获取监控启动状态的chan
	ccid := ChaincodeID(sysCC.Name())
	done := chaincodeStreamHandler.LaunchInProc(ccid)

	//创建两个peer节点与_lifecycle相互收发消息的chan，一个用于“peer发，chaincode收”，一个用于“chaincode发，peer收”
	peerRcvCCSend := make(chan *pb.ChaincodeMessage)
	ccRcvPeerSend := make(chan *pb.ChaincodeMessage)

	//启动peer端与_lifecycle交互的协程
	go func() {
		//使用上文创建的两个chan，创建peer端与_lifecycle通信的对象，这里标记为PEER_STREAM
		stream := newInProcStream(peerRcvCCSend, ccRcvPeerSend)
		defer stream.CloseSend()

		sysccLogger.Debugf("starting chaincode-support stream for  %s", ccid)
		//使用PEER_STREAM启动与_lifecycle的通信进程。
		err := chaincodeStreamHandler.HandleChaincodeStream(stream)
		sysccLogger.Criticalf("shim stream ended with err: %v", err)
	}()

	//启动_lifecycle与peer端交互的协程。
	go func(sysCC SelfDescribingSysCC) {
		//使用上文创建的两个chan，创建_lifecycle与peer端通信的对象，这里标记为CC_STREAM
		stream := newInProcStream(ccRcvPeerSend, peerRcvCCSend)
		defer stream.CloseSend()

		sysccLogger.Debugf("chaincode started for %s", ccid)
		//创建处理peer端消息的Handler，并使用通信对象启动与peer端通信的进程。
		//创建用于处理peer端消息的Handler，这里标记为LIFECC_ HANDLER，stream为与peer节点通信的对象，cc为_lifecycle链码对象
		//向peer端发送一条“注册”消息，告之_lifecycle自身已开始启动，可以开始初始化。
		err := shim.StartInProc(ccid, stream, sysCC.Chaincode())
		sysccLogger.Criticalf("system chaincode ended with err: %v", err)
	}(sysCC)
	<-done // 监听_lifecycle的启动状态，等待其准备就绪。
}
