package proxy

import (
	"bufio"
	"github.com/yinqiwen/gsnova/src/event"
	"fmt"
	"io"
	"log"
	"github.com/yinqiwen/gsnova/src/misc/socks"
	"net"
	"net/http"
	"strings"
)

const (
	HTTP_TUNNEL  = 1
	HTTPS_TUNNEL = 2
	SOCKS_TUNNEL = 3

	STATE_RECV_HTTP       = 1
	STATE_RECV_HTTP_CHUNK = 2
	STATE_RECV_TCP        = 3
	STATE_SESSION_CLOSE   = 4

	GLOBAL_PROXY_SERVER = 1
	GAE_PROXY_SERVER    = 2
	C4_PROXY_SERVER     = 3
	SSH_PROXY_SERVER    = 4

	GAE_NAME                 = "GAE"
	C4_NAME                  = "C4"
	GOOGLE_NAME              = "Google"
	GOOGLE_HTTP_NAME         = "GoogleHttp"
	GOOGLE_HTTPS_NAME        = "GoogleHttps"
	GOOGLE_HTTPS_DIRECT_NAME = "GoogleHttpsDirect"
	FORWARD_NAME             = "Forward"
	SSH_NAME                 = "SSH"
	AUTO_NAME                = "Auto"
	DIRECT_NAME              = "Direct"
	DEFAULT_NAME             = "Default"

	ATTR_REDIRECT_HTTPS = "RedirectHttps"
	ATTR_CRLF_INJECT    = "CRLF"
	ATTR_DIRECT         = "Direct"
	ATTR_TUNNEL         = "Tunnel"
	ATTR_RANGE          = "Range"
	ATTR_SYS_DNS        = "SysDNS"
	ATTR_APP            = "App"

	MODE_HTTP    = "http"
	MODE_HTTPS   = "httpS"
	MODE_RSOCKET = "rsocket"
	MODE_XMPP    = "xmpp"
)

var total_proxy_conn_num uint32

type RemoteConnection interface {
	Request(conn *SessionConnection, ev event.Event) (err error, res event.Event)
	GetConnectionManager() RemoteConnectionManager
	Close() error
}

type RemoteConnectionManager interface {
	GetRemoteConnection(ev event.Event, attrs map[string]string) (RemoteConnection, error)
	RecycleRemoteConnection(conn RemoteConnection)
	GetName() string
}

type SessionConnection struct {
	SessionID       uint32
	LocalBufferConn *bufio.Reader
	LocalRawConn    net.Conn
	RemoteConn      RemoteConnection
	State           uint32
	Type            uint32
	ProxyServerType int
}

func newSessionConnection(sessionId uint32, conn net.Conn, reader *bufio.Reader) *SessionConnection {
	session_conn := new(SessionConnection)
	session_conn.LocalRawConn = conn
	session_conn.LocalBufferConn = reader
	session_conn.SessionID = sessionId
	session_conn.State = STATE_RECV_HTTP
	session_conn.Type = HTTP_TUNNEL
	return session_conn
}

func (session *SessionConnection) Close() error {
	if nil != session.LocalRawConn {
		session.LocalRawConn.Close()
	}
	if nil != session.RemoteConn {
		session.RemoteConn.Close()
	}
	return nil
}

func (session *SessionConnection) tryProxy(proxies []RemoteConnectionManager, attrs map[string]string, ev *event.HTTPRequestEvent) (err error) {
	for _, proxy := range proxies {
		session.RemoteConn, err = proxy.GetRemoteConnection(ev, attrs)
		if nil == err {
			err, _ = session.RemoteConn.Request(session, ev)
		}
		if nil == err {
			return nil
		} else {
			log.Printf("Session[%d][WARN][%s]Failed to request proxy event for reason:%v", session.SessionID, proxy.GetName(), err)
		}
	}
	return fmt.Errorf("No proxy found for request '%s %s' with %d candidates", ev.RawReq.Method, ev.RawReq.Host, len(proxies))
}

func (session *SessionConnection) processHttpEvent(ev *event.HTTPRequestEvent) error {
	ev.SetHash(session.SessionID)
	//proxies, attrs := SelectProxy(ev.RawReq, session.LocalRawConn, session.Type == HTTPS_TUNNEL)
	proxies, attrs := SelectProxy(ev.RawReq, session)
	if nil == proxies {
		session.State = STATE_SESSION_CLOSE
		return nil
	}
	var err error
	if nil == session.RemoteConn {
		err = session.tryProxy(proxies, attrs, ev)
	} else {
		rmanager := session.RemoteConn.GetConnectionManager()
		matched := false
		for _, proxy := range proxies {
			proxyName := adjustProxyName(proxy.GetName(), session.Type == HTTPS_TUNNEL)
			if rmanager.GetName() == proxyName {
				matched = true
				break
			}
		}
		if !matched {
			session.RemoteConn.Close()
			err = session.tryProxy(proxies, attrs, ev)
		} else {
			err, _ = session.RemoteConn.Request(session, ev)
		}
	}

	if nil != err {
		log.Printf("Session[%d]Process error:%v for host:%s", session.SessionID, err, ev.RawReq.Host)
		session.LocalRawConn.Write([]byte("HTTP/1.1 500 Internal Server Error\r\n\r\n"))
		session.LocalRawConn.Close()
	}
	return nil
}

func (session *SessionConnection) processHttpChunkEvent(ev *event.HTTPChunkEvent) error {
	ev.SetHash(session.SessionID)
	if nil != session.RemoteConn {
		session.RemoteConn.Request(session, ev)
	}
	return nil
}

func (session *SessionConnection) process() error {
	close_session := func() {
		session.LocalRawConn.Close()
		if nil != session.RemoteConn {
			session.RemoteConn.Close()
		}
		session.State = STATE_SESSION_CLOSE
	}

	readRequest := func() (*http.Request, error) {
		req, e := http.ReadRequest(session.LocalBufferConn)
		if nil != req {
			req.Header.Del("Proxy-Connection")
		}
		return req, e
	}

	switch session.State {
	case STATE_RECV_HTTP:
		req, rerr := readRequest()
		if nil == rerr {
			var rev event.HTTPRequestEvent
			rev.FromRequest(req)
			rev.SetHash(session.SessionID)
			err := session.processHttpEvent(&rev)
			if err != nil {
				log.Printf("Session[%d]Failed to read http request:%v\n", session.SessionID, err)
				close_session()
				return io.EOF
			}
		}
		if nil != rerr {
			log.Printf("Session[%d]Browser close connection:%v\n", session.SessionID, rerr)
			close_session()
			return io.EOF
		}
	case STATE_RECV_HTTP_CHUNK:
		buf := make([]byte, 8192)
		n, err := session.LocalBufferConn.Read(buf)
		if nil == err {
			rev := new(event.HTTPChunkEvent)
			rev.Content = buf[0:n]
			err = session.processHttpChunkEvent(rev)
		}
		if nil != err {
			close_session()

			return io.EOF
		}
	case STATE_RECV_TCP:

	}
	return nil
}

type ForwardSocksDialer struct {
	proxyPort int
}

func (f *ForwardSocksDialer) DialTCP(n string, laddr *net.TCPAddr, raddr string) (net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", f.proxyPort))
	if nil == err {
		_, port, er := net.SplitHostPort(raddr)
		if nil == er && port != "80" {
			req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Connection: Keep-Alive\r\n\r\n", raddr, raddr)
			conn.Write([]byte(req))
			tmp := make([]byte, 1024)
			conn.Read(tmp)
		}
	}
	return conn, err
}

func HandleConn(sessionId uint32, conn net.Conn, proxyServerType int) {
	total_proxy_conn_num = total_proxy_conn_num + 1
	defer func() {
		total_proxy_conn_num = total_proxy_conn_num - 1
	}()
	bufreader := bufio.NewReader(conn)
	b, err := bufreader.Peek(1)
	if nil != err {
		if err != io.EOF {
			log.Printf("Failed to peek data:%s\n", err.Error())
		}
		conn.Close()
		return
	}
	localAddr := conn.LocalAddr().(*net.TCPAddr)
	if b[0] == byte(4) || b[0] == byte(5) {
		socks.ServConn(bufreader, conn.(*net.TCPConn), &ForwardSocksDialer{proxyPort: localAddr.Port})
		return
	}
	b, err = bufreader.Peek(7)
	if nil != err {
		if err != io.EOF {
			log.Printf("Failed to peek data:%s\n", err.Error())
		}
		conn.Close()
		return
	}

	session := newSessionConnection(sessionId, conn, bufreader)
	session.ProxyServerType = proxyServerType
	if strings.EqualFold(string(b), "Connect") {
		session.Type = HTTPS_TUNNEL
	} else {
		session.Type = HTTP_TUNNEL
	}
	for session.State != STATE_SESSION_CLOSE {
		err := session.process()
		if nil != err {
			break
		}
	}
}
