package tmqp

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"net"
	"os"
	"strings"
)

const (
	DELIMITER byte = '\n'
	TYPE      uint = 1
	SIZE      uint = 4
	VERSION        = "TMQP 0.1\n"
)

func Handshake() net.Conn {

	conn, err := net.Dial("tcp", ":7788")
	checkError(err)
	//defer conn.Close()

	handleConn(conn)

	return conn
}

func handleConn(conn net.Conn) {

	fmt.Println("STARTING!")

	conn.Write([]byte("TMQP 0.1\n"))

	msg, err := bufio.NewReader(conn).ReadString('\n')

	if strings.Compare(msg, "START\n") == 0 {
		_, err = conn.Write([]byte("START OK\n"))
	}

	checkError(err)
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func SendPackage(conn net.Conn, type_package int8) {

	fmt.Println("TYPE pck: ", type_package)

	type_byte := make([]byte, TYPE)
	type_byte[0] = (byte(type_package))

	// _, err := conn.Write(type_byte)
	err := binary.Write(conn, binary.LittleEndian, type_byte)

	if err == nil {

		fmt.Println("ENVIANDO PACOTE...")
		fmt.Println("TYPE: ", type_byte)

		if type_package == 1 {

			pacote := &QueueDeclare{
				Name:      "FILA-1",
				Exclusive: false,
			}

			data, _ := proto.Marshal(pacote)

			size_package := uint32(len(data))
			size_bytes := make([]byte, SIZE)
			binary.LittleEndian.PutUint32(size_bytes, uint32(size_package))

			fmt.Println("SIZE: ", size_bytes)

			err := binary.Write(conn, binary.LittleEndian, size_bytes)

			if err != nil {

				fmt.Println("ERROR - WRITE SIZE: ", err)

			} else {

				fmt.Println(data)
				_, err := conn.Write([]byte(data))

				if err != nil {
					fmt.Println("ERROR - WRITE DATA: ", err)
				} else {
					fmt.Println("PACOTE ENVIADO...")
				}

			}
		}

	} else {
		fmt.Println("ERROR - WRITE TYPE: ", err)
	}

}
