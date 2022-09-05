package main

import (
    "bufio"
    "fmt"
    "net"
    "os"
    "strconv"
    "time"
)

func main() {
    l, err := net.Listen("tcp", "0.0.0.0:6379")
    if err != nil {
        fmt.Println("Failed to bind to port 6379")
        os.Exit(1)
    }

    storage := NewStorage()

    for {
        conn, err := l.Accept()
        if err != nil {
            fmt.Println("Error accepting connection: ", err.Error())
            os.Exit(1)
        }

        go handleConnection(conn, storage)
    }
}

func handleConnection(conn net.Conn, storage *Storage) {
    defer conn.Close()

    for {
        value, err := DecodeRESP(bufio.NewReader(conn))
        if err != nil {
            fmt.Println("Error decoding RESP: ", err.Error())
            return
        }

        command := value.Array()[0].String()
        args := value.Array()[1:]

        switch command {
        case "set":
            handleSetCommand(conn, args, storage)
        case "get":
            handleGetCommand(conn, args, storage)
        case "ping":
            conn.Write([]byte("+PONG\r\n"))
        case "echo":
            conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(args[0].String()), args[0].String())))
        default:
            conn.Write([]byte("-ERR unknown command '" + command + "'\r\n"))
        }
    }
}

func handleSetCommand(conn net.Conn, args []Value, storage *Storage) {
    if len(args) > 2 {
        // px option is to set the specified expire time in milliseconds
        if args[2].String() == "px" {
            expiryStr := args[3].String()
            expiryInMilliseconds, err := strconv.Atoi(expiryStr)
            if err != nil {
                conn.Write([]byte(fmt.Sprintf("-ERR PX value (%s) is not an integer\r\n", expiryStr)))
            }

            storage.SetWithExpiry(args[0].String(), args[1].String(), time.Duration(expiryInMilliseconds) * time.Millisecond)
        } else {
            conn.Write([]byte(fmt.Sprintf("-ERR unknown option for set: %s\r\n", args[2].String())))
        }
    } else {
        storage.Set(args[0].String(), args[1].String())
    }

    conn.Write([]byte("+OK\r\n"))
}

func handleGetCommand(conn net.Conn, args []Value, storage *Storage) {
    value, found := storage.Get(args[0].String())

    if found {
        conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
    } else {
        conn.Write([]byte("$-1\r\n"))
    }
}
