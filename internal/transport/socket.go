package transport

import (
    "bufio"
    "encoding/binary"
    "io"
    "net"
)

// ServeUnix accepts a handler for net.Conn
func ServeUnix(socketPath string, handler func(net.Conn)) error {
    l, err := net.Listen("unix", socketPath)
    if err != nil {
        return err
    }
    defer l.Close()

    for {
        conn, err := l.Accept()
        if err != nil {
            return err
        }
        go handler(conn)
    }
}

// Simple framing helpers
func ReadMessage(conn net.Conn) ([]byte, error) {
    reader := bufio.NewReader(conn)
    lengthBytes := make([]byte, 4)
    if _, err := io.ReadFull(reader, lengthBytes); err != nil {
        return nil, err
    }
    length := binary.BigEndian.Uint32(lengthBytes)

    data := make([]byte, length)
    if _, err := io.ReadFull(reader, data); err != nil {
        return nil, err
    }
    return data, nil
}

func WriteMessage(conn net.Conn, data []byte) error {
    length := make([]byte, 4)
    binary.BigEndian.PutUint32(length, uint32(len(data)))
    if _, err := conn.Write(length); err != nil {
        return err
    }
    _, err := conn.Write(data)
    return err
}
