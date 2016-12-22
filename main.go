package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	//"github.com/varnish/vcs-streamer/targets"
	"log"
	"net"
)

type Entry struct {
	Key     string   `json:"key,omitempty"`
	Buckets []Bucket `json:"buckets,omitempty"`
}

type Bucket struct {
	Timestamp   string `json:"timestamp,omitempty"`
	Nrequests   string `json:"n_requests,omitempty"`
	NreqUniq    string `json:"n_req_uniq,omitempty"`
	Nmisses     string `json:"n_misses,omitempty"`
	Nrestarts   string `json:"n_restarts,omitempty"`
	TTFBmiss    string `json:"ttfb_miss,omitempty"`
	TTFBhit     string `json:"ttfb_hit,omitempty"`
	NbodyBytes  string `json:"n_bodybytes,omitempty"`
	RespBytes   string `json:"respbytes,omitempty"`
	ReqBytes    string `json:"reqbytes,omitempty"`
	BeReqBytes  string `json:"bereqbytes,omitempty"`
	BeRespBytes string `json:"berespbytes,omitempty"`
	RespCode1xx string `json:"resp_code_1xx,omitempty"`
	RespCode2xx string `json:"resp_code_2xx,omitempty"`
	RespCode3xx string `json:"resp_code_3xx,omitempty"`
	RespCode4xx string `json:"resp_code_4xx,omitempty"`
	RespCode5xx string `json:"resp_code_5xx,omitempty"`
}

func handler(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	split := func(data []byte, atEOF bool) (int, []byte, error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.Index(data, []byte{'\n', '\n'}); i >= 0 {
			// We have a full event
			return i + 2, data[0:i], nil
		}
		// If we're at EOF, we have a final event
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	}
	scanner.Split(split)

	for {
		// Set the split function for the scanning operation.
		if scanner.Scan() {
			entry := scanner.Bytes()
			//log.Println("New event")

			// Remove the first line of the entry, that contains the number
			// of bytes to read.
			e := Entry{}
			entry = entry[bytes.IndexByte(entry, '\n'):]

			if err := json.Unmarshal(entry, &e); err != nil {
				log.Fatalf("Error decoding data: %s\n", err)
			}

			fmt.Printf("Key: %s\n", e.Key)
			fmt.Printf("%v\n\n", e)
		}
	}
}

func main() {
	l, err := net.Listen("tcp", ":1337")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go handler(conn)
	}
}
