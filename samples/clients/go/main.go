// Publish one message, then fetch and ack it.
//
//	go run .
//
// NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

const (
	opHello = 1
	opHelloOK = 2
	opEnsureStream = 3
	opEnsureStreamOK = 4
	opPublish = 5
	opPublishOK = 6
	opEnsureConsumer = 7
	opEnsureConsumerOK = 8
	opFetch = 9
	opFetchOK = 10
	opAck = 11
	opAckOK = 12
	opError = 21
)

type writer struct{ buf []byte }

func (w *writer) u8(v int)  { w.buf = append(w.buf, byte(v)) }
func (w *writer) u16(v int) { w.buf = binary.LittleEndian.AppendUint16(w.buf, uint16(v)) }
func (w *writer) i32(v int) { w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(int32(v))) }
func (w *writer) i64(v int64) {
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], uint64(v))
	w.buf = append(w.buf, raw[:]...)
}
func (w *writer) str(v string) {
	w.u16(len(v))
	w.buf = append(w.buf, v...)
}
func (w *writer) blob(v []byte) {
	w.i32(len(v))
	w.buf = append(w.buf, v...)
}

type reader struct {
	data []byte
	i    int
}

func (r *reader) take(n int) ([]byte, error) {
	if r.i+n > len(r.data) {
		return nil, fmt.Errorf("frame ended early")
	}
	chunk := r.data[r.i : r.i+n]
	r.i += n
	return chunk, nil
}
func (r *reader) u8() (int, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return int(b[0]), nil
}
func (r *reader) u16() (int, error) {
	b, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint16(b)), nil
}
func (r *reader) i32() (int, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return int(int32(binary.LittleEndian.Uint32(b))), nil
}
func (r *reader) i64() (int64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}
func (r *reader) str() (string, error) {
	n, err := r.u16()
	if err != nil {
		return "", err
	}
	b, err := r.take(n)
	return string(b), err
}
func (r *reader) blob() ([]byte, error) {
	n, err := r.i32()
	if err != nil {
		return nil, err
	}
	return r.take(n)
}

type client struct {
	conn   net.Conn
	nextID uint32
}

func (c *client) request(op int, payload []byte, expected int) ([]byte, error) {
	c.nextID++
	body := make([]byte, 6+len(payload))
	binary.LittleEndian.PutUint16(body[0:2], uint16(op))
	binary.LittleEndian.PutUint32(body[2:6], c.nextID)
	copy(body[6:], payload)
	head := make([]byte, 4)
	binary.LittleEndian.PutUint32(head, uint32(len(body)))
	if _, err := c.conn.Write(append(head, body...)); err != nil {
		return nil, err
	}
	var sizeRaw [4]byte
	if _, err := io.ReadFull(c.conn, sizeRaw[:]); err != nil {
		return nil, err
	}
	frame := make([]byte, binary.LittleEndian.Uint32(sizeRaw[:]))
	if _, err := io.ReadFull(c.conn, frame); err != nil {
		return nil, err
	}
	kind := int(binary.LittleEndian.Uint16(frame[0:2]))
	requestID := binary.LittleEndian.Uint32(frame[2:6])
	data := frame[6:]
	if requestID != c.nextID {
		return nil, fmt.Errorf("response id did not match the request")
	}
	if kind == opError {
		rd := reader{data: data}
		code, _ := rd.u16()
		message, _ := rd.str()
		return nil, fmt.Errorf("broker error %d: %s", code, message)
	}
	if kind != expected {
		return nil, fmt.Errorf("unexpected frame %d", kind)
	}
	return data, nil
}

func (c *client) hello(user, password, vhost, name string) error {
	var w writer
	w.str("")
	w.str(name)
	w.str(user)
	w.str(password)
	w.str(vhost)
	_, err := c.request(opHello, w.buf, opHelloOK)
	return err
}

func (c *client) ensureStream(name string, filters []string) error {
	var w writer
	w.str(name)
	w.u16(len(filters))
	for _, filter := range filters {
		w.str(filter)
	}
	w.u16(1)
	w.i64(-1)
	w.i64(-1)
	w.i32(0)
	_, err := c.request(opEnsureStream, w.buf, opEnsureStreamOK)
	return err
}

func (c *client) publish(subject string, payload []byte, key string) (string, int, int64, error) {
	var w writer
	w.str(subject)
	w.str(key)
	w.u16(0)
	w.blob(payload)
	data, err := c.request(opPublish, w.buf, opPublishOK)
	if err != nil {
		return "", 0, 0, err
	}
	rd := reader{data: data}
	count, err := rd.u16()
	if err != nil || count < 1 {
		return "", 0, 0, fmt.Errorf("publish was not stored")
	}
	stream, err := rd.str()
	if err != nil {
		return "", 0, 0, err
	}
	partition, err := rd.u16()
	if err != nil {
		return "", 0, 0, err
	}
	offset, err := rd.i64()
	return stream, partition, offset, err
}

func (c *client) ensureConsumer(stream, name string) error {
	var w writer
	w.str(stream)
	w.str(name)
	w.str("")
	w.i32(30000)
	w.i32(5)
	w.i32(1000)
	w.u8(0)
	w.u8(0)
	w.i64(0)
	_, err := c.request(opEnsureConsumer, w.buf, opEnsureConsumerOK)
	return err
}

func (c *client) fetch(stream, name string) ([][3]any, error) {
	var w writer
	w.str(stream)
	w.str(name)
	w.u16(32)
	w.i32(0)
	data, err := c.request(opFetch, w.buf, opFetchOK)
	if err != nil {
		return nil, err
	}
	rd := reader{data: data}
	reset, err := rd.u8()
	if err != nil {
		return nil, err
	}
	if reset == 1 {
		at, _ := rd.i64()
		return nil, fmt.Errorf("offset reset at %d", at)
	}
	if _, err = rd.i64(); err != nil {
		return nil, err
	}
	count, err := rd.u16()
	if err != nil {
		return nil, err
	}
	messages := make([][3]any, 0, count)
	for i := 0; i < count; i++ {
		partition, err := rd.u16()
		if err != nil {
			return nil, err
		}
		offset, err := rd.i64()
		if err != nil {
			return nil, err
		}
		if _, err = rd.i64(); err != nil {
			return nil, err
		}
		if _, err = rd.i32(); err != nil {
			return nil, err
		}
		if _, err = rd.str(); err != nil {
			return nil, err
		}
		if _, err = rd.blob(); err != nil {
			return nil, err
		}
		headers, err := rd.u16()
		if err != nil {
			return nil, err
		}
		for h := 0; h < headers; h++ {
			if _, err = rd.str(); err != nil {
				return nil, err
			}
			if _, err = rd.blob(); err != nil {
				return nil, err
			}
		}
		payload, err := rd.blob()
		if err != nil {
			return nil, err
		}
		messages = append(messages, [3]any{partition, offset, string(payload)})
	}
	return messages, nil
}

func (c *client) ack(stream, name string, partition int, offset int64) error {
	var w writer
	w.str(stream)
	w.str(name)
	w.u16(partition)
	w.i64(offset)
	_, err := c.request(opAck, w.buf, opAckOK)
	return err
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func main() {
	if len(os.Args) > 1 && os.Args[1] != "demo" {
		runFeature(os.Args[1])
		return
	}
	language := "go"
	body := "hello from " + language
	conn, err := net.Dial("tcp", env("NUVEXA_HOST", "127.0.0.1")+":"+env("NUVEXA_PORT", "5761"))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	c := &client{conn: conn}
	if err = c.hello("guest", "guest", "/", "sample-"+language); err != nil {
		fatal(err)
	}
	if err = c.ensureStream("clients", []string{"clients.>"}); err != nil {
		fatal(err)
	}
	stream, partition, offset, err := c.publish("clients.created", []byte(body), language)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("published %s partition %d offset %d\n", stream, partition, offset)
	consumer := "demo-" + language
	if err = c.ensureConsumer("clients", consumer); err != nil {
		fatal(err)
	}
	for {
		messages, err := c.fetch("clients", consumer)
		if err != nil {
			fatal(err)
		}
		if len(messages) == 0 {
			fatal(fmt.Errorf("published message was not delivered"))
		}
		found := false
		for _, message := range messages {
			part := message[0].(int)
			messageOffset := message[1].(int64)
			text := message[2].(string)
			fmt.Printf("fetched %d %s\n", messageOffset, text)
			if err = c.ack("clients", consumer, part, messageOffset); err != nil {
				fatal(err)
			}
			found = found || text == body
		}
		if found {
			return
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
