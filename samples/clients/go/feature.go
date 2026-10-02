package main

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
)

const (
	featHello = 1
	featHelloOK = 2
	featEnsureStream = 3
	featEnsureStreamOK = 4
	featPublish = 5
	featPublishOK = 6
	featEnsureConsumer = 7
	featEnsureConsumerOK = 8
	featFetch = 9
	featFetchOK = 10
	featAck = 11
	featAckOK = 12
	featNack = 13
	featNackOK = 14
	featReset = 15
	featResetOK = 16
	featRelease = 17
	featReleaseOK = 18
	featPing = 19
	featPong = 20
	featError = 21
	featDeclareExchange = 22
	featDeclareExchangeOK = 23
	featDeclareQueue = 24
	featDeclareQueueOK = 25
	featBind = 26
	featBindOK = 27
	featDeleteQueue = 28
	featDeleteQueueOK = 29
	featDeleteExchange = 30
	featDeleteExchangeOK = 31
	featPurge = 32
	featPurgeOK = 33
	featPublishExchange = 34
	featPublishExchangeOK = 35
)

type session struct {
	conn net.Conn
	next uint32
}

type receipt struct {
	stream string
	partition int
	offset int64
}

type delivery struct {
	partition int
	offset int64
	attempts int32
	payload []byte
}

func runFeature(command string) {
	language := "go"
	if command == "admin" {
		admin(language)
		return
	}
	conn, err := net.Dial("tcp", env("NUVEXA_HOST", "127.0.0.1")+":"+env("NUVEXA_PORT", "5761"))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	s := &session{conn: conn}
	s.hello("sample-" + language)
	switch command {
	case "ping":
		s.ping()
	case "stream":
		runStream(s, language)
	case "consume":
		runConsume(s, language)
	case "exchange":
		runExchange(s, language)
	default:
		fatal(fmt.Errorf("use demo, ping, stream, consume, exchange, or admin"))
	}
}

func runStream(s *session, language string) {
	stream := "catalog-" + language
	s.ensureStream(stream, []string{stream + ".>"}, 2, 86400000, 1048576, 65536)
	body := []byte(`{"id":1}`)
	headers := []header{{"content-type", []byte("application/json")}}
	keyed := s.publish(stream+".created", body, "alpha", headers)
	first := s.publish(stream+".created", body, "", nil)
	second := s.publish(stream+".created", body, "", nil)
	if len(keyed) == 0 || first[0].partition == second[0].partition {
		fatal(fmt.Errorf("round-robin did not use both partitions"))
	}
}

func runConsume(s *session, language string) {
	stream := "mailbox-" + language
	durable := "box-" + language
	tail := "tail-" + language
	s.ensureStream(stream, []string{stream + ".>"}, 1, -1, -1, 0)
	s.ensureConsumer(stream, durable, stream+".>", false, 0, 0)
	plain := []header{{"content-type", []byte("text/plain")}}
	s.publish(stream+".created", []byte("ack me"), "", plain)
	s.publish(stream+".created", []byte("nack me"), "", plain)
	found := map[string]delivery{}
	for _, message := range s.fetch(stream, durable, 32) {
		found[string(message.payload)] = message
	}
	s.ack(stream, durable, found["ack me"])
	fmt.Println("ack")
	s.nack(stream, durable, found["nack me"])
	fmt.Println("nack")
	var again *delivery
	for _, message := range s.fetch(stream, durable, 32) {
		if string(message.payload) == "nack me" {
			copy := message
			again = &copy
		}
	}
	if again == nil {
		fatal(fmt.Errorf("nack was not redelivered"))
	}
	fmt.Printf("redelivered %d\n", again.attempts)
	s.ack(stream, durable, *again)
	s.reset(stream, durable, 0)
	reset := s.fetch(stream, durable, 1)
	if len(reset) == 0 {
		fatal(fmt.Errorf("reset did not return a message"))
	}
	fmt.Printf("after reset offset %d\n", reset[0].offset)
	s.ack(stream, durable, reset[0])
	s.release(stream, tail)
	s.ensureConsumer(stream, tail, "", true, 1, 0)
	s.publish(stream+".created", []byte("tail me"), "", nil)
	var tailed *delivery
	for _, message := range s.fetch(stream, tail, 32) {
		if string(message.payload) == "tail me" {
			copy := message
			tailed = &copy
		}
	}
	if tailed == nil {
		fatal(fmt.Errorf("tail message was not delivered"))
	}
	s.ack(stream, tail, *tailed)
	s.release(stream, tail)
	s.ensureConsumer(stream, "from0-"+language, "", false, 2, 0)
	fmt.Println("offset consumer from0-" + language)
}

func runExchange(s *session, language string) {
	queue := "work-" + language
	dead := "dead-" + language
	direct := "direct-" + language
	fanout := "fanout-" + language
	topic := "topic-" + language
	headersName := "headers-" + language
	s.declareExchange("amq.direct", "direct")
	s.declareExchange(direct, "direct")
	s.declareExchange(fanout, "fanout")
	s.declareExchange(topic, "topic")
	s.declareExchange(headersName, "headers")
	s.declareQueue(dead, -1, -1, "", "")
	s.declareQueue(queue, 60000, 100, direct, "expired")
	s.bind(direct, dead, "expired", nil)
	s.bind(direct, queue, "work.created", nil)
	s.bind(fanout, queue, "", nil)
	s.bind(topic, queue, "work.*", nil)
	s.bind(headersName, queue, "", [][2]string{{"format", "json"}, {"x-match", "all"}})
	count := 0
	count += len(s.publishExchange(direct, "work.created", []byte("routed"), "order-1", nil))
	count += len(s.publishExchange(fanout, "", []byte("routed"), "", nil))
	count += len(s.publishExchange(topic, "work.created", []byte("routed"), "", nil))
	count += len(s.publishExchange(headersName, "", []byte("routed"), "", []header{{"format", []byte("json")}}))
	count += len(s.publishExchange("", queue, []byte("routed"), "", nil))
	if count < 5 {
		fatal(fmt.Errorf("expected at least 5 receipts, got %d", count))
	}
	s.purge(queue)
	s.deleteQueue(queue)
	s.deleteQueue(dead)
	for _, name := range []string{direct, fanout, topic, headersName} {
		s.deleteExchange(name)
	}
}

type header struct {
	name string
	value []byte
}

func (s *session) hello(name string) {
	w := &bytes.Buffer{}
	putStr(w, env("NUVEXA_TOKEN", ""))
	putStr(w, name)
	putStr(w, env("NUVEXA_USER", "guest"))
	putStr(w, env("NUVEXA_PASSWORD", "guest"))
	putStr(w, env("NUVEXA_VHOST", "/"))
	r := featReader{data: s.request(featHello, w.Bytes(), featHelloOK)}
	fmt.Println("hello " + r.str())
}

func (s *session) ping() {
	s.request(featPing, nil, featPong)
	fmt.Println("pong")
}

func (s *session) ensureStream(name string, filters []string, partitions int, maxAge, maxBytes int64, maxMessage int32) {
	w := &bytes.Buffer{}
	putStr(w, name)
	putU16(w, len(filters))
	for _, filter := range filters {
		putStr(w, filter)
	}
	putU16(w, partitions)
	putI64(w, maxAge)
	putI64(w, maxBytes)
	putI32(w, maxMessage)
	s.request(featEnsureStream, w.Bytes(), featEnsureStreamOK)
	fmt.Printf("stream %s partitions %d\n", name, partitions)
}

func (s *session) publish(subject string, payload []byte, key string, headers []header) []receipt {
	return s.receipts(s.request(featPublish, message(subject, key, headers, payload), featPublishOK))
}

func (s *session) ensureConsumer(stream, name, filter string, ephemeral bool, start int, offset int64) {
	w := &bytes.Buffer{}
	putStr(w, stream)
	putStr(w, name)
	putStr(w, filter)
	putI32(w, 30000)
	putI32(w, 5)
	putI32(w, 1000)
	flag := 0
	if ephemeral {
		flag = 1
	}
	putU8(w, flag)
	putU8(w, start)
	putI64(w, offset)
	s.request(featEnsureConsumer, w.Bytes(), featEnsureConsumerOK)
	fmt.Printf("consumer %s on %s start %d\n", name, stream, start)
}

func (s *session) fetch(stream, name string, max int) []delivery {
	w := &bytes.Buffer{}
	putStr(w, stream)
	putStr(w, name)
	putU16(w, max)
	putI32(w, 0)
	r := featReader{data: s.request(featFetch, w.Bytes(), featFetchOK)}
	if r.u8() == 1 {
		fatal(fmt.Errorf("offset reset"))
	}
	r.i64()
	count := r.u16()
	messages := make([]delivery, 0, count)
	for i := 0; i < count; i++ {
		item := delivery{partition: r.u16(), offset: r.i64()}
		r.i64()
		item.attempts = r.i32()
		r.str()
		r.blob()
		headers := r.u16()
		for h := 0; h < headers; h++ {
			r.str()
			r.blob()
		}
		item.payload = r.blob()
		messages = append(messages, item)
	}
	return messages
}

func (s *session) ack(stream, name string, message delivery) { s.cursor(featAck, featAckOK, stream, name, message) }
func (s *session) nack(stream, name string, message delivery) { s.cursor(featNack, featNackOK, stream, name, message) }

func (s *session) reset(stream, name string, offset int64) {
	w := &bytes.Buffer{}
	putStr(w, stream)
	putStr(w, name)
	putU8(w, 1)
	putI64(w, offset)
	s.request(featReset, w.Bytes(), featResetOK)
	fmt.Printf("reset %s offset %d\n", name, offset)
}

func (s *session) release(stream, name string) {
	w := &bytes.Buffer{}
	putStr(w, stream)
	putStr(w, name)
	s.request(featRelease, w.Bytes(), featReleaseOK)
	fmt.Printf("release %s\n", name)
}

func (s *session) declareExchange(name, kind string) {
	w := &bytes.Buffer{}
	putStr(w, name)
	putStr(w, kind)
	putU8(w, 1)
	putU8(w, 0)
	s.request(featDeclareExchange, w.Bytes(), featDeclareExchangeOK)
	fmt.Printf("exchange %s %s\n", name, kind)
}

func (s *session) declareQueue(name string, ttl int64, max int32, dead, deadKey string) {
	w := &bytes.Buffer{}
	putStr(w, name)
	putU8(w, 1)
	putU8(w, 0)
	putU8(w, 0)
	putI64(w, ttl)
	putI32(w, max)
	putStr(w, dead)
	putStr(w, deadKey)
	s.request(featDeclareQueue, w.Bytes(), featDeclareQueueOK)
	fmt.Printf("queue %s\n", name)
}

func (s *session) bind(exchange, queue, routingKey string, arguments [][2]string) {
	w := &bytes.Buffer{}
	putStr(w, exchange)
	putStr(w, queue)
	putStr(w, routingKey)
	putU16(w, len(arguments))
	for _, argument := range arguments {
		putStr(w, argument[0])
		putStr(w, argument[1])
	}
	s.request(featBind, w.Bytes(), featBindOK)
}

func (s *session) publishExchange(exchange, routingKey string, payload []byte, key string, headers []header) []receipt {
	w := &bytes.Buffer{}
	putStr(w, exchange)
	putStr(w, routingKey)
	putStr(w, key)
	putU16(w, len(headers))
	for _, header := range headers {
		putStr(w, header.name)
		putBlob(w, header.value)
	}
	putBlob(w, payload)
	receipts := s.receipts(s.request(featPublishExchange, w.Bytes(), featPublishExchangeOK))
	fmt.Printf("routed %s -> %d\n", exchange, len(receipts))
	return receipts
}

func (s *session) purge(name string) { s.named(featPurge, featPurgeOK, name, "purge queue "+name) }
func (s *session) deleteQueue(name string) { s.named(featDeleteQueue, featDeleteQueueOK, name, "delete queue "+name) }
func (s *session) deleteExchange(name string) { s.named(featDeleteExchange, featDeleteExchangeOK, name, "delete exchange "+name) }

func (s *session) cursor(op, expected uint16, stream, name string, message delivery) {
	w := &bytes.Buffer{}
	putStr(w, stream)
	putStr(w, name)
	putU16(w, message.partition)
	putI64(w, message.offset)
	s.request(op, w.Bytes(), expected)
}

func (s *session) named(op, expected uint16, name, label string) {
	w := &bytes.Buffer{}
	putStr(w, name)
	s.request(op, w.Bytes(), expected)
	fmt.Println(label)
}

func (s *session) receipts(data []byte) []receipt {
	r := featReader{data: data}
	count := r.u16()
	out := make([]receipt, 0, count)
	for i := 0; i < count; i++ {
		item := receipt{stream: r.str(), partition: r.u16(), offset: r.i64()}
		fmt.Printf("published %s partition %d offset %d\n", item.stream, item.partition, item.offset)
		out = append(out, item)
	}
	return out
}

func message(subject, key string, headers []header, payload []byte) []byte {
	w := &bytes.Buffer{}
	putStr(w, subject)
	putStr(w, key)
	putU16(w, len(headers))
	for _, header := range headers {
		putStr(w, header.name)
		putBlob(w, header.value)
	}
	putBlob(w, payload)
	return w.Bytes()
}

func (s *session) request(op uint16, payload []byte, expected uint16) []byte {
	s.next++
	body := &bytes.Buffer{}
	putU16(body, int(op))
	var id [4]byte
	binary.LittleEndian.PutUint32(id[:], s.next)
	body.Write(id[:])
	body.Write(payload)
	frame := &bytes.Buffer{}
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(body.Len()))
	frame.Write(length[:])
	frame.Write(body.Bytes())
	if _, err := s.conn.Write(frame.Bytes()); err != nil {
		fatal(err)
	}
	size := readExact(s.conn, 4)
	data := readExact(s.conn, int(binary.LittleEndian.Uint32(size)))
	kind := binary.LittleEndian.Uint16(data)
	requestID := binary.LittleEndian.Uint32(data[2:])
	if requestID != s.next {
		fatal(fmt.Errorf("response id did not match the request"))
	}
	payloadOut := data[6:]
	if kind == featError {
		r := featReader{data: payloadOut}
		fatal(fmt.Errorf("broker error %d: %s", r.u16(), r.str()))
	}
	if kind != expected {
		fatal(fmt.Errorf("unexpected frame %d", kind))
	}
	return payloadOut
}

func readExact(conn net.Conn, count int) []byte {
	buf := make([]byte, count)
	if _, err := io.ReadFull(conn, buf); err != nil {
		fatal(err)
	}
	return buf
}

func putU8(b *bytes.Buffer, value int) { b.WriteByte(byte(value)) }
func putU16(b *bytes.Buffer, value int) {
	var raw [2]byte
	binary.LittleEndian.PutUint16(raw[:], uint16(value))
	b.Write(raw[:])
}
func putI32(b *bytes.Buffer, value int32) {
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], uint32(value))
	b.Write(raw[:])
}
func putI64(b *bytes.Buffer, value int64) {
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], uint64(value))
	b.Write(raw[:])
}
func putStr(b *bytes.Buffer, value string) {
	putU16(b, len(value))
	b.WriteString(value)
}
func putBlob(b *bytes.Buffer, value []byte) {
	putI32(b, int32(len(value)))
	b.Write(value)
}

type featReader struct {
	data []byte
	i int
}

func (r *featReader) take(count int) []byte {
	if r.i+count > len(r.data) {
		fatal(fmt.Errorf("frame ended early"))
	}
	chunk := r.data[r.i : r.i+count]
	r.i += count
	return chunk
}
func (r *featReader) u8() int { return int(r.take(1)[0]) }
func (r *featReader) u16() int { return int(binary.LittleEndian.Uint16(r.take(2))) }
func (r *featReader) i32() int32 { return int32(binary.LittleEndian.Uint32(r.take(4))) }
func (r *featReader) i64() int64 { return int64(binary.LittleEndian.Uint64(r.take(8))) }
func (r *featReader) str() string { return string(r.take(r.u16())) }
func (r *featReader) blob() []byte { return append([]byte(nil), r.take(int(r.i32()))...) }

func admin(language string) {
	host := env("NUVEXA_HOST", "127.0.0.1")
	health, _ := strconv.Atoi(env("NUVEXA_HEALTH_PORT", "5762"))
	management, _ := strconv.Atoi(env("NUVEXA_MANAGEMENT_PORT", "5763"))
	httpsPort, _ := strconv.Atoi(env("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764"))
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	user := env("NUVEXA_USER", "guest")
	password := env("NUVEXA_PASSWORD", "guest")
	call := func(method, url string, body any, auth bool) {
		var raw []byte
		if body != nil {
			raw, _ = json.Marshal(body)
		}
		req, err := http.NewRequest(method, url, bytes.NewReader(raw))
		if err != nil {
			fatal(err)
		}
		if auth {
			req.SetBasicAuth(user, password)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		if err != nil {
			fatal(err)
		}
		defer resp.Body.Close()
		text, _ := io.ReadAll(resp.Body)
		fmt.Printf("%s %s %d %d bytes\n", method, url, resp.StatusCode, len(text))
		if resp.StatusCode >= 400 {
			fatal(fmt.Errorf("%s", text))
		}
	}
	call(http.MethodGet, fmt.Sprintf("http://%s:%d/health", host, health), nil, false)
	call(http.MethodGet, fmt.Sprintf("http://%s:%d/metrics", host, health), nil, false)
	base := fmt.Sprintf("http://%s:%d", host, management)
	for _, path := range []string{"/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies"} {
		call(http.MethodGet, base+path, nil, true)
	}
	vhost := "vh-" + language
	account := "user-" + language
	policy := "policy-" + language
	call(http.MethodPut, base+"/api/vhosts/"+vhost, nil, true)
	call(http.MethodPut, base+"/api/users/"+account, map[string]any{"password": "sample-pass", "tags": []string{"management"}}, true)
	call(http.MethodPut, base+"/api/permissions", map[string]any{"user": account, "vhost": vhost, "configure": ".*", "write": ".*", "read": ".*"}, true)
	call(http.MethodPut, base+"/api/policies/"+policy, map[string]any{"vhost": "/", "pattern": "sample-.*", "priority": 1, "messageTtlMs": 60000, "maxLength": 100, "deadLetterExchange": "", "deadLetterRoutingKey": ""}, true)
	call(http.MethodDelete, base+"/api/policies/"+policy+"?vhost=/", nil, true)
	call(http.MethodDelete, base+"/api/permissions?user="+account+"&vhost="+vhost, nil, true)
	call(http.MethodDelete, base+"/api/users/"+account, nil, true)
	call(http.MethodDelete, base+"/api/vhosts/"+vhost, nil, true)
	call(http.MethodGet, fmt.Sprintf("https://%s:%d/api/whoami", host, httpsPort), nil, true)
}
