package client

import (
	"encoding/binary"
	"errors"
	"github.com/darwinOrg/go-common/context"
	pool "github.com/darwinOrg/go-conn-pool"
	dgcron "github.com/darwinOrg/go-cron"
	dglogger "github.com/darwinOrg/go-logger"
	"net"
	"time"
)

type PubClient struct {
	host     string
	port     int
	timeout  time.Duration
	cron     *dgcron.DgCron
	connPool pool.Pool
}

func NewPubClient(host string, port int, timeout time.Duration, poolSize int) (*PubClient, error) {
	if poolSize < 1 {
		poolSize = 1
	}

	cron := dgcron.NewAndStart(nil)

	channelPool, err := pool.NewChannelPool(1, poolSize, func() (net.Conn, error) {
		nw, err := newNetwork(host, port, timeout)
		if err != nil {
			return nil, err
		}

		cron.AddJob("smss发布客户端连接保活", "0 0/10 * * * ?", func(ctx *dgctx.DgContext) {
			err := alive(nw.conn, timeout, ctx.TraceId)
			if err != nil {
				dglogger.Errorf(ctx, "smss alive error: %v", err)
			}
		})

		return nw.conn, nil
	})
	if err != nil {
		return nil, err
	}

	return &PubClient{
		host:     host,
		port:     port,
		timeout:  timeout,
		cron:     cron,
		connPool: channelPool,
	}, nil
}

func (pc *PubClient) getConn() (net.Conn, error) {
	return pc.connPool.Get()
}

func (pc *PubClient) Close() {
	pc.cron.Stop()
	pc.connPool.Close()
}

func (pc *PubClient) Publish(mqName string, msg *Message, traceId string) error {
	conn, ne := pc.getConn()
	if ne != nil {
		return ne
	}
	defer conn.Close()

	payload := msg.ToBytes()
	payLoadLen := len(payload)
	if payLoadLen <= 8 {
		return errors.New("empty message")
	}
	traceIdLen := len(traceId)
	buf := make([]byte, HeaderSize+len(mqName)+payLoadLen+traceIdLen)
	buf[0] = CommandPub.Byte()
	buf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(mqName)))
	binary.LittleEndian.PutUint32(buf[3:], uint32(payLoadLen))

	dst := buf[HeaderSize:]
	n := copy(dst, mqName)
	dst = dst[n:]
	if traceIdLen > 0 {
		n = copy(dst, traceId)
		dst = dst[n:]
	}
	copy(dst, payload)
	payload = nil

	if err := writeAll(conn, buf, pc.timeout); err != nil {
		return err
	}

	buf = nil

	return readResult(conn, pc.timeout)
}

func (pc *PubClient) PublishDelay(mqName string, msg *Message, delayMils int64, traceId string) error {
	conn, ne := pc.getConn()
	if ne != nil {
		return ne
	}
	defer conn.Close()
	payload := msg.ToBytes()
	payLoadLen := len(payload)
	if payLoadLen <= 8 {
		return errors.New("empty message")
	}
	traceIdLen := len(traceId)
	buf := make([]byte, HeaderSize+len(mqName)+len(payload)+12+traceIdLen)
	buf[0] = CommandDelay.Byte()
	buf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(mqName)))
	binary.LittleEndian.PutUint32(buf[3:], uint32(payLoadLen))

	dst := buf[HeaderSize:]
	n := copy(dst, mqName)
	dst = dst[n:]
	if traceIdLen > 0 {
		n = copy(dst, traceId)
		dst = dst[n:]
	}

	binary.LittleEndian.PutUint64(dst, uint64(delayMils))

	copy(dst[8:], payload)
	payload = nil

	if err := writeAll(conn, buf, pc.timeout); err != nil {
		return err
	}

	buf = nil

	return readResult(conn, pc.timeout)
}

func (pc *PubClient) CreateMQ(mqName string, life int64, traceId string) error {
	conn, ne := pc.getConn()
	if ne != nil {
		return ne
	}
	defer conn.Close()
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandCreateMQ.Byte()
	hBuf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(hBuf[1:], uint16(len(mqName)))
	hBuf = append(hBuf, []byte(mqName)...)

	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}

	pBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(pBuf, uint64(life))
	hBuf = append(hBuf, pBuf...)

	if err := writeAll(conn, hBuf, pc.timeout); err != nil {
		return err
	}

	return readResult(conn, pc.timeout)
}

func (pc *PubClient) DeleteMQ(mqName string, traceId string) error {
	conn, ne := pc.getConn()
	if ne != nil {
		return ne
	}
	defer conn.Close()
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandDeleteMQ.Byte()
	hBuf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(hBuf[1:], uint16(len(mqName)))
	hBuf = append(hBuf, []byte(mqName)...)
	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}
	if err := writeAll(conn, hBuf, pc.timeout); err != nil {
		return err
	}

	return readResult(conn, pc.timeout)
}

func (pc *PubClient) GetMqList(traceId string) (string, error) {
	conn, ne := pc.getConn()
	if ne != nil {
		return "", ne
	}
	defer conn.Close()
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandList.Byte()
	hBuf[19] = byte(traceIdLen)
	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}
	if err := writeAll(conn, hBuf, pc.timeout); err != nil {
		return "", err
	}
	return readMqListResult(conn, pc.timeout)
}

func alive(conn net.Conn, timeout time.Duration, traceId string) error {
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandAlive.Byte()
	hBuf[19] = byte(traceIdLen)
	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}
	if err := writeAll(conn, hBuf, timeout); err != nil {
		return err
	}
	return readResult(conn, timeout)
}

func readResult(conn net.Conn, timeout time.Duration) error {
	buf := make([]byte, RespHeaderSize)
	if err := readAll(conn, buf, timeout); err != nil {
		return err
	}

	code := binary.LittleEndian.Uint16(buf)
	if code == OkCode || code == AliveCode {
		return nil
	}
	if code != ErrCode {
		return errors.New("not support code")
	}
	errMsgLen := int(binary.LittleEndian.Uint16(buf[2:]))
	return readErrCodeMsg(conn, errMsgLen, timeout)
}

func readMqListResult(conn net.Conn, timeout time.Duration) (string, error) {
	buf := make([]byte, RespHeaderSize)
	if err := readAll(conn, buf, timeout); err != nil {
		return "", err
	}

	code := binary.LittleEndian.Uint16(buf)
	if code == OkCode {
		bodyLen := int(binary.LittleEndian.Uint32(buf[2:]))
		return readMQListBody(conn, bodyLen, timeout)
	}
	if code != ErrCode {
		return "", errors.New("not suport code")
	}
	errMsgLen := int(binary.LittleEndian.Uint16(buf[2:]))
	return "", readErrCodeMsg(conn, errMsgLen, timeout)
}
