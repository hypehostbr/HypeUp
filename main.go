package main

import (
	"archive/tar"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/joho/godotenv"
	"github.com/klauspost/pgzip"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)
var (
	pbKey string
	shKey string
	bucket string
	endpoint string
	daemonPath string
	ACL string
)

var (
	WarningLogger *log.Logger
	InfoLogger *log.Logger
	PanicLogger *log.Logger
)

func init() {
	godotenv.Load()

	pbKey = os.Getenv("pb_key")
	shKey = os.Getenv("sh_key")
	bucket = os.Getenv("bucket")
	endpoint = os.Getenv("endpoint")
	daemonPath = os.Getenv("daemon_path")
	ACL = os.Getenv("public_access")

	logWarn, err := os.OpenFile("./logs/warn.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)

	logInfo, err := os.OpenFile("./logs/info.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)

	logPanic, err := os.OpenFile("./logs/panic.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)

	if err != nil {
		panic(err)
		return
	}

	WarningLogger = log.New(logWarn, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	InfoLogger = log.New(logInfo, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	PanicLogger = log.New(logPanic, "PANIC: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	if ACL == "yes" {
		ACL = "public-read"
	} else { ACL = "private" }

   dirs, err := ioutil.ReadDir(daemonPath)

   if err != nil {
   	   PanicLogger.Println("Can't read the daemon dir. daemon_path: ", daemonPath)
	   return
   }
   for _, dir := range dirs {
   		if dir.IsDir() {
   			upload(daemonPath + dir.Name())
		}
   }

}

func upload(serverPath string) {
	s3Config := &aws.Config{
		Credentials: credentials.NewStaticCredentials(pbKey, shKey, ""),
		Endpoint: aws.String(endpoint),
		Region: aws.String("us-east-1"),
	}

	session, err := session.NewSession(s3Config)
	if err != nil {
		PanicLogger.Println("Can't create a session with the API. is credentials correct?", err)
		return
	}

	read, writer := io.Pipe()

	go compress(serverPath, writer, read)

	uploader := s3manager.NewUploader(session)



	result, err := uploader.Upload(&s3manager.UploadInput{
		Body: read,
		Bucket: aws.String(bucket),
		ACL: aws.String(ACL),
		Key: aws.String(filepath.Base(serverPath) + ".tar.gz"),
	})

	if err != nil {
		WarningLogger.Printf("Can't upload a backup to Bucket. Server_name: %v, Server_path: %v\n Full error: %v ", filepath.Base(serverPath), serverPath, err)
		return
	}

	InfoLogger.Printf("Backup has been uploaded successfully! Server_name: %v, Server_path: %v Location: %v\n", filepath.Base(serverPath), serverPath, result.Location)
}

func compress(serverPath string, Writer *io.PipeWriter, Reader *io.PipeReader) {
	InfoLogger.Printf("Compressing has been started! Server_name: %v, Server_path: %v\n", filepath.Base(serverPath), serverPath)

	gtzip := pgzip.NewWriter(Writer)
	tw := tar.NewWriter(gtzip)

	err := filepath.Walk(serverPath, func(file string, fi os.FileInfo, err error) error {
		header, err := tar.FileInfoHeader(fi, file)

		header.Name = filepath.ToSlash(file)

		err = tw.WriteHeader(header)

		if err != nil {
			WarningLogger.Printf("Failed to compress a server folder. Server_name: %v, Server_path: %v\n", filepath.Base(serverPath), serverPath)
			return err
		}

		if !fi.Mode().IsRegular() {
			return nil
		}

		if !fi.IsDir() {
			fileData, err := os.Open(file)

			if err != nil {
				WarningLogger.Printf("Failed to open file. Do i have permission to open it? Server_name: %v, Server_path: %v\n", filepath.Base(serverPath), serverPath)
				return err
			}

			_, err = io.Copy(tw, fileData)

			if err != nil {
				WarningLogger.Printf("Failed to compress the file. The data from a file can't be copied to tarball. Server_name: %v, Server_path: %v\n", filepath.Base(serverPath), serverPath)
				return err
			}
			fileData.Close()
		}

		return nil
	})

	if err != nil {
		WarningLogger.Println("An error has been reported from compress func. Full error: ", err)
		Reader.CloseWithError(err)
	}

	tw.Close()
	gtzip.Close()
	Writer.Close()
	InfoLogger.Printf("Server has been compressed! Server_name: %v, Server_path: %v\n",  filepath.Base(serverPath), serverPath)
}
