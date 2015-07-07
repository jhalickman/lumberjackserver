package lumberjackserver

import (
	"compress/zlib"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
)

type FileEvent struct {
	Source string
	Offset int
	Host   string
	Text   string
	Fields map[string]string
}

type Server struct {
	SSLCertificate string
	SSLKey         string
	Port           string
	EventHandler   func(f *FileEvent)
}

func NewServer(cert, key, port string) *Server {
	s := &Server{SSLCertificate: cert, SSLKey: key, Port: port}
	return s
}

func (s *Server) Serve() error {

	var tlsconfig tls.Config
	tlsconfig.MinVersion = tls.VersionTLS10

	cert, err := tls.LoadX509KeyPair(s.SSLCertificate, s.SSLKey)
	if err != nil {
		return fmt.Errorf("Failed loading client ssl certificate: %s", err)
	}
	tlsconfig.Certificates = []tls.Certificate{cert}

	tlsconfig.Rand = rand.Reader
	service := fmt.Sprintf("0.0.0.0:%s", s.Port)
	listener, err := tls.Listen("tcp", service, &tlsconfig)
	if err != nil {
		return err
	}

	log.Print("server: listening")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("server: accept: %s", err)
			break
		}
		defer conn.Close()
		log.Printf("server: accepted from %s", conn.RemoteAddr())
		go s.handleClient(conn)
	}
	return nil
}

func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()
	for {
		log.Print("server: conn: waiting")

		err := s.handleMessage(conn)
		if err != nil {
			log.Println(err)
			break
		}
	}
	log.Println("server: conn: closed")
}

func (s *Server) handleMessage(conn net.Conn) error {
	header := make([]byte, 2)
	var eventCount, compressedSize uint32

	var reader io.Reader
	reader = conn

	for {
		_, err := reader.Read(header)
		if err != nil {
			return fmt.Errorf("server: conn: handleMessage: %s", err)
		}
		log.Printf("server: conn: header: %s\n", string(header))
		switch string(header) {
		case "1W":
			binary.Read(reader, binary.BigEndian, &eventCount)
			log.Printf("server: conn: 1W length: %d\n", eventCount)
		case "1C":
			binary.Read(reader, binary.BigEndian, &compressedSize)
			log.Printf("server: conn: 1C length: %d\n", compressedSize)
			decompressor, err := zlib.NewReader(reader)
			defer decompressor.Close()
			if err != nil {
				return fmt.Errorf("server: conn: handleMessage: %s", err)
			}
			reader = decompressor
		case "1D":
			err := s.handleData(reader, conn)
			if err != nil {
				return fmt.Errorf("server: conn: handleMessage: %s", err)
			}
			eventCount--
		}

		if eventCount == 0 {
			return nil
		}
	}
}

func (s *Server) handleData(reader io.Reader, writer io.Writer) error {
	var sequence, pairs uint32
	binary.Read(reader, binary.BigEndian, &sequence)
	binary.Read(reader, binary.BigEndian, &pairs)

	log.Printf("server: conn: sequence: %d\n", sequence)
	event := FileEvent{Fields: make(map[string]string)}
	var i uint32
	for i = 0; i < pairs; i++ {
		var key_len, val_len uint32
		binary.Read(reader, binary.BigEndian, &key_len)
		key := make([]byte, key_len)
		reader.Read(key)
		binary.Read(reader, binary.BigEndian, &val_len)
		val := make([]byte, val_len)
		reader.Read(val)
		//log.Printf("[%d] %s : %s", i, string(key), string(val))
		switch string(key) {
		case "file":
			event.Source = string(val)
		case "host":
			event.Host = string(val)
		case "offset":
			offset, err := strconv.Atoi(string(val))
			if err != nil {
				return err
			}
			event.Offset = offset
		case "line":
			event.Text = string(val)
		default:
			event.Fields[string(key)] = string(val)
		}
	}

	s.EventHandler(&event)
	writer.Write([]byte("1A"))
	binary.Write(writer, binary.BigEndian, sequence)

	return nil
}
