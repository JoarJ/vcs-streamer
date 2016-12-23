package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/varnish/vcs-streamer/output"
	"log"
	"net"
	"strconv"
)

var (
	hostFlag   = flag.String("listen-host", "127.0.0.1", "Listen host")
	portFlag   = flag.Int("listen-port", 6556, "Listen port")
	outputFlag = flag.String("output", "collectd", "Specified output")
	intervalFlag = flag.Uint("interval", 10, "Interval in seconds")
)

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

			// Remove the first line of the entry, that
			// contains the number of bytes to read.
			e := output.Entry{}
			entry = entry[bytes.IndexByte(entry, '\n'):]

			if err := json.Unmarshal(entry, &e); err != nil {
				log.Printf("Invalid data: %s\n", entry)
				log.Fatalf("Decode error: %s\n", err)
			}

			//fmt.Printf("Key: %s\n", e.Key)
			//fmt.Printf("%v\n\n", e)
			for _, b := range e.Buckets {
				k := e.Key + "/n_requests"
				t := b.Timestamp
				v := b.Nrequests
				i := *intervalFlag
				fmt.Printf("PUTVAL %s interval=%d %s:%s\n", k, i, t, v)
			}
		}
	}
}

func main() {
	flag.Parse()

	l, err := net.Listen("tcp", *hostFlag+":"+strconv.Itoa(*portFlag))
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
