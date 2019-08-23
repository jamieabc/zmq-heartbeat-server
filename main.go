package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	_ "github.com/influxdata/influxdb1-client"
	zmq "github.com/pebbe/zmq4"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	broadcasterZapDomain    = "broadcaster"
	privateKeyFile          = "peer.private"
	publicKeyFile           = "peer.public"
	host                    = "127.0.0.1"
	port                    = "9999"
	maximumPacketSize       = 5000000 // 5 MB
	chain                   = "testing"
	broadcastIntervalSecond = 60 * time.Second
)

type connection struct {
	ip   net.IP
	port uint16
}

func (c *connection) String() string {
	return fmt.Sprintf("tcp://%s:%d", c.ip.String(), c.port)
}

func main() {
	if !isValidKeyFiles() {
		printHelp()
		return
	}

	zmq.AuthSetVerbose(false)
	err := zmq.AuthStart()
	if nil != err {
		panic(err)
	}

	private, err := ioutil.ReadFile(privateKeyFile)
	if nil != err {
		panic(err)
	}
	fmt.Printf("private: %s\n", string(private))
	privateKey, _ := hex.DecodeString(string(private))

	public, err := ioutil.ReadFile(publicKeyFile)
	if nil != err {
		panic(err)
	}
	fmt.Printf("public: %s\n", string(public))
	publicKey, _ := hex.DecodeString(string(public))

	c, err := newConnection(fmt.Sprintf("%s:%s", host, port))

	fmt.Printf("connection: %s\n", c.String())
	socket4, err := bind(zmq.PUB, broadcasterZapDomain, privateKey, publicKey, c)
	if nil != err {
		fmt.Printf("bind socket with error: %s\n", err)
	}

	timer := time.NewTimer(broadcastIntervalSecond)

	for {
		select {
		case <-timer.C:
			err = heartbeat(socket4)
			fmt.Printf("%s send heartbeat\n", time.Now())
			if nil != err {
				fmt.Printf("send heartbeat with error: %s\n", err)
			}
			timer.Reset(broadcastIntervalSecond)
		}
	}

}

func isValidKeyFiles() bool {
	if _, err := os.Stat(privateKeyFile); os.IsNotExist(err) {
		return false
	}
	if _, err := os.Stat(publicKeyFile); os.IsNotExist(err) {
		return false
	}

	return true
}

func printHelp() {
	fmt.Println("Usage: zmqHeartbeat")
	fmt.Println("key file: peer.private & peer.public, each 64 characters long")
	fmt.Println("host: 127.0.0.1, port: 9999")
	fmt.Println("every zmq communication starts with prefix: testing")
}

func newConnection(hostPort string) (*connection, error) {
	host, port, err := net.SplitHostPort(hostPort)
	if nil != err {
		return nil, errors.New("invalid length")
	}

	IP := net.ParseIP(strings.Trim(host, " "))
	if nil == IP {
		ips, err := net.LookupIP(host)
		if nil != err {
			return nil, err
		}
		if len(ips) < 1 {
			return nil, errors.New("invalid ip or port")
		}
		IP = ips[0]
	}

	numericPort, err := strconv.Atoi(strings.Trim(port, " "))
	if nil != err {
		return nil, err
	}
	if numericPort < 1 || numericPort > 65535 {
		return nil, errors.New("invalid port")
	}
	c := &connection{
		ip:   IP,
		port: uint16(numericPort),
	}
	return c, nil
}

func bind(socketType zmq.Type, zapDomain string, privateKey []byte, publicKey []byte, listen *connection) (*zmq.Socket, error) {

	socket4 := (*zmq.Socket)(nil)
	err := error(nil)

	socket4, err = newServerSocket(socketType, zapDomain, privateKey, publicKey)
	if nil != err {
		fmt.Printf("new server socket with error: %s\n", err)
		goto fail
	}

	fmt.Printf("listen: %s\n", listen.String())
	err = socket4.Bind(listen.String())
	if nil != err {
		fmt.Printf("bind socket with error: %s\n", err)
		goto fail
	}

	return socket4, nil

fail:
	return nil, err
}

func newServerSocket(socketType zmq.Type, zapDomain string, privateKey []byte, publicKey []byte) (*zmq.Socket, error) {

	socket, err := zmq.NewSocket(socketType)
	if nil != err {
		return nil, err
	}

	// allow any client to connect
	//zmq.AuthAllow(zapDomain, "127.0.0.1/8")
	//zmq.AuthAllow(zapDomain, "::1")
	zmq.AuthCurveAdd(zapDomain, zmq.CURVE_ALLOW_ANY)

	// domain is servers public key
	socket.SetCurveServer(1)
	//socket.SetCurvePublickey(publicKey)
	socket.SetCurveSecretkey(string(privateKey))

	err = socket.SetZapDomain(zapDomain)
	if nil != err {
		goto failure
	}

	socket.SetIdentity(string(publicKey)) // just use public key for identity

	err = socket.SetIpv6(false) // conditionally set IPv6 state
	if nil != err {
		goto failure
	}

	// only queue message to connected peers
	socket.SetImmediate(true)
	socket.SetLinger(100 * time.Millisecond)

	err = socket.SetSndtimeo(120 * time.Second)
	if nil != err {
		goto failure
	}
	err = socket.SetRcvtimeo(120 * time.Second)
	if nil != err {
		goto failure
	}

	err = socket.SetTcpKeepalive(1)
	if nil != err {
		goto failure
	}
	err = socket.SetTcpKeepaliveCnt(5)
	if nil != err {
		goto failure
	}
	err = socket.SetTcpKeepaliveIdle(60)
	if nil != err {
		goto failure
	}
	err = socket.SetTcpKeepaliveIntvl(60)
	if nil != err {
		goto failure
	}

	err = socket.SetMaxmsgsize(maximumPacketSize)
	if nil != err {
		goto failure
	}

	return socket, nil

failure:
	return nil, err
}

func heartbeat(socket *zmq.Socket) error {
	_, err := socket.Send(chain, zmq.SNDMORE|zmq.DONTWAIT)
	if nil != err {
		fmt.Printf("send chain with error: %s\n", err)
		return err
	}

	_, err = socket.Send("heart", zmq.SNDMORE|zmq.DONTWAIT)
	if nil != err {
		fmt.Printf("send command with error: %s\n", err)
		return err
	}

	_, err = socket.SendBytes([]byte{'b', 'e', 'a', 't'}, zmq.DONTWAIT)
	if nil != err {
		fmt.Printf("send beat with error: %s\n", err)
		return err
	}

	return nil
}
