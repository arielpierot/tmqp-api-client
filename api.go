package tmqp_api

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func Handshake() {

	conn, err := net.Dial("tcp", ":7788")
	checkError(err)
	defer conn.Close()

	handleConn(conn)

	os.Exit(0)
}

func handleConn(conn net.Conn) {

	fmt.Println("STARTING!")

	conn.Write([]byte("TMQP 0.1\n"))

	msg, err := bufio.NewReader(conn).ReadString('\n')

	if strings.Compare(msg, "START\n") == 0 {
		_, err = conn.Write([]byte("START OK\n"))
		for {
			_, err = conn.Write([]byte("TESTE!\n"))
		}
	}

	checkError(err)
	conn.Close()
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
