package ravendb

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/ravendb/ravendb-go-client"
	"github.com/ravendb/ravendb-go-client/serverwide/certificates"
	"github.com/ravendb/ravendb-go-client/serverwide/operations"
	internal_operations "github.com/ravendb/terraform-provider-ravendb/operations"
	"github.com/spf13/cast"
	"golang.org/x/crypto/ssh"
	"log"
	"net"
	"net/url"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	NUMBER_OF_RETRIES                       int    = 5
	DEFAULT_SECURE_RAVENDB_HTTP_PORT        int    = 443
	DEFAULT_USECURED_RAVENDB_HTTP_PORT      int    = 8080
	DEFAULT_SECURE_RAVENDB_TCP_PORT         int    = 38888
	DEFAULT_UNSECURED_RAVENDB_TCP_PORT      int    = 38881
	DEFAULT_HTTP_PORT                       int    = 80
	CREDENTIALS_FOR_SECURE_STORE_FIELD_NAME string = "store"
)

type ServerConfig struct {
	Package             Package                       `json:"Package"`
	Hosts               []string                      `json:"Hosts,omitempty"`
	Settings            map[string]interface{}        `json:"Settings,omitempty"`
	ClusterSetupZip     map[string]*CertificateHolder `json:"ClusterSetupZip,omitempty"`
	Url                 Url                           `json:"Url"`
	Assets              map[string][]byte             `json:"Assets,omitempty"`
	Unsecured           bool                          `json:"Unsecured,omitempty"`
	SSH                 SSH                           `json:"SSH"`
	HealthcheckDatabase string                        `json:"HealthcheckDatabase,omitempty"`
	Databases           []Database                    `json:"Databases,omitempty"`
	DatabasesToDelete   []DatabaseToDelete            `json:"DatabasesToDelete,omitempty"`
	IndexesToDelete     []IndexesToDelete             `json:"IndexesToDelete,omitempty"`
}

type CertificateHolder struct {
	Pfx          []byte `json:"Pfx,omitempty"`
	Cert         []byte `json:"Cert,omitempty"`
	Key          []byte `json:"Key,omitempty"`
	SettingsJson []byte `json:"SettingsJson,omitempty"`
	License      []byte `json:"Licence,omitempty"`
}

type NodeState struct {
	Host              string                       `json:"Host,omitempty"`
	Settings          map[string]interface{}       `json:"Settings,omitempty"`
	ClusterSetupZip   map[string]CertificateHolder `json:"ClusterSetupZip,omitempty"`
	Assets            map[string][]byte            `json:"Assets,omitempty"`
	Unsecured         bool                         `json:"Unsecured,omitempty"`
	Version           string                       `json:"Version,omitempty"`
	Failed            bool                         `json:"Failed,omitempty"`
	Databases         []Database                   `json:"Databases,omitempty"`
	DatabasesToDelete []DatabaseToDelete           `json:"DatabasesToDelete,omitempty"`
	IndexesToDelete   []IndexesToDelete            `json:"IndexesToDelete,omitempty"`
	Licence           []byte
}

type IndexesToDelete struct {
	DatabaseName string   `json:"DatabaseName,omitempty"`
	IndexesNames []string `json:"IndexesNames,omitempty"`
}

type DatabaseToDelete struct {
	Name       string `json:"Name,omitempty"`
	HardDelete bool   `json:"HardDelete,omitempty"`
}

type Database struct {
	Name             string            `json:"Name,omitempty"`
	Settings         map[string]string `json:"Settings,omitempty"`
	ReplicationNodes []string          `json:"ReplicationNodes,omitempty"`
	Key              string            `json:"Key,omitempty"`
	Indexes          []Index           `json:"Indexes,omitempty"`
}

type Index struct {
	IndexName     string            `json:"IndexName,omitempty"`
	Maps          []string          `json:"Maps,omitempty"`
	Reduce        string            `json:"Reduce,omitempty"`
	Configuration map[string]string `json:"Configuration,omitempty"`
}
type params struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}
type Package struct {
	Version string `json:"Version,omitempty"`
	Arch    string `json:"Arch,omitempty"`
}

type Url struct {
	List     []string `json:"List,omitempty"`
	HttpPort int      `json:"HttpPort,omitempty"`
	TcpPort  int      `json:"TcpPort,omitempty"`
}

type SSH struct {
	User string `json:"User,omitempty"`
	Pem  []byte `json:"Pem,omitempty"`
	Port int    `json:"Port,omitempty"`
}

func (s *SSH) getPort() int {
	if s.Port != 0 {
		return s.Port
	}
	return 22
}

type DeployError struct {
	Output string
	Err    error
}

func (idx *Index) convertConfiguration() map[string]string {
	m := make(map[string]string)

	for k, v := range idx.Configuration {
		strKey := fmt.Sprintf("%v", k)
		strValue := fmt.Sprintf("%v", v)
		m[strKey] = strValue
	}
	return m
}

func (e *DeployError) Error() string {
	return e.Err.Error() + " with output:\n" + e.Output
}

func upload(con *ssh.Client, buf bytes.Buffer, path string, content []byte) error {
	//https://chuacw.ath.cx/development/b/chuacw/archive/2019/02/04/how-the-scp-protocol-works.aspx
	session, err := con.NewSession()
	if err != nil {
		return err
	}

	buf.WriteString("sudo scp -t " + path + "\n")

	stdin, err := session.StdinPipe()
	if err != nil {
		return err
	}
	go func() {
		defer stdin.Close()
		fmt.Fprint(stdin, "C0660 "+strconv.Itoa(len(content))+" file\n")
		stdin.Write(content)
		fmt.Fprint(stdin, "\x00")
	}()

	output, err := session.CombinedOutput("sudo scp -t " + path)
	buf.Write(output)
	if err != nil {
		return &DeployError{
			Err:    err,
			Output: buf.String(),
		}
	}

	session.Close()

	session, err = con.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	buf.WriteString("sudo chown ravendb:ravendb " + path + "\n")

	output, err = session.CombinedOutput("sudo chown ravendb:ravendb " + path)
	buf.Write(output)
	if err != nil {
		return errors.New("Failed to ownership: " + path + "\n" + err.Error() + "\n")
	}

	return nil
}

func (sc *ServerConfig) deployRavenDbInstances(parallel bool) error {
	var wg sync.WaitGroup
	errorsChannel := make(chan error, len(sc.Hosts))
	sc.Url.List = make([]string, len(sc.Hosts))

	wg.Add(len(sc.Hosts))
	for index, publicIp := range sc.Hosts {
		deployAction := func(copyOfPublicIp string, copyOfIndex int) {
			err := sc.deployServer(copyOfPublicIp, copyOfIndex)
			if err != nil {
				errorsChannel <- err
			}
			wg.Done()
		}
		if parallel {
			go deployAction(publicIp, index)
		} else {
			deployAction(publicIp, index)
		}
	}

	wg.Wait()
	close(errorsChannel)

	var result error

	for err := range errorsChannel {
		result = multierror.Append(result, err)
	}
	return result
}

func listFiles(conn *ssh.Client, dir string) ([]string, error) {
	session, err := conn.NewSession()
	if err != nil {
		return nil, err
	}
	output, err := session.CombinedOutput("sudo find '" + dir + "' -type f  -maxdepth 1")
	if err != nil {
		return nil, errors.New(string(output))
	}
	str := string(output)
	lines := strings.Split(str, "\n")
	return lines[:len(lines)-1], nil
}

func (sc *ServerConfig) ReadServer(publicIP string, index int) (NodeState, error) {
	var stdoutBuf bytes.Buffer
	var ns NodeState
	var conn *ssh.Client
	var store *ravendb.DocumentStore
	defer func() {
		log.Println(stdoutBuf.String())
	}()

	signer, err := ssh.ParsePrivateKey(sc.SSH.Pem)
	if err != nil {
		return ns, err
	}

	authConfig := &ssh.ClientConfig{
		User:            sc.SSH.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Second * 10,
	}

	conn, err = sc.ConnectToRemoteWithRetry(publicIP, conn, authConfig)
	if err != nil {
		return ns, err
	}

	defer conn.Close()
	files, err := listFiles(conn, "/etc/ravendb")
	if err != nil {
		return ns, err
	}
	ns.Assets = make(map[string][]byte)
	for _, file := range files {
		_, fileName := filepath.Split(file)

		contents, err := readFileContents(file, stdoutBuf, conn)
		if err != nil {
			return ns, err
		}
		if contents == nil {
			contents = make([]byte, 0) // empty file
		}
		ns.Assets[fileName] = contents
	}

	files, err = listFiles(conn, "/etc/ravendb/security")
	if err != nil {
		return ns, err
	}
	for _, file := range files {
		_, fileName := filepath.Split(file)

		contents, err := readFileContents(file, stdoutBuf, conn)
		if err != nil {
			return ns, err
		}
		if contents == nil {
			contents = make([]byte, 0) // empty file
		}
		ns.Assets[fileName] = contents
	}
	delete(ns.Assets, "master.key")

	ns.Settings = make(map[string]interface{})
	if file, ok := ns.Assets["settings.json"]; ok {
		err = json.Unmarshal(file, &ns.Settings)
		if err != nil {
			stdoutBuf.WriteString("Failed to parse settings.json\n")
			stdoutBuf.Write(file)
			return ns, err
		}
		delete(ns.Assets, "settings.json")
	}
	//workaround to convert unmarshalled map[string]interface{} values to string.
	for key := range ns.Settings {
		ns.Settings[key] = fmt.Sprintf("%v", ns.Settings[key])
	}

	if license, ok := ns.Assets["license.json"]; ok {
		ns.Licence = license
		delete(ns.Assets, "license.json")
	}

	if cert, ok := ns.Assets["server.pfx"]; ok {
		idx := string(rune(index + 'A'))
		var certHolder CertificateHolder
		ns.ClusterSetupZip = make(map[string]CertificateHolder)
		certHolder.Pfx = cert
		ns.ClusterSetupZip[idx] = certHolder
		delete(ns.Assets, "server.pfx")
	}

	name := CREDENTIALS_FOR_SECURE_STORE_FIELD_NAME
	if val, ok := sc.ClusterSetupZip[name]; ok {
		store, err = getStore(sc, *val)
		if err != nil {
			return ns, err
		}
	} else {
		idx := string(rune(index + 'A'))
		store, err = getStore(sc, ns.ClusterSetupZip[idx])
		if err != nil {
			return ns, err
		}
	}

	buildNumber := operations.OperationGetBuildNumber{}
	err = executeWithRetries(store, &buildNumber)
	if err != nil {
		return ns, err
	}

	ns.Version = strconv.Itoa(buildNumber.BuildVersion)

	ns.Host = publicIP
	//ns.TcpUrl = ns.Settings["PublicServerUrl"].(string)
	//ns.HttpUrl = ns.Settings["PublicServerUrl.Tcp"].(string)
	//if unsecuredAccessAllowed, ok := ns.Settings["Security.UnsecuredAccessAllowed"]; ok {
	//	ns.Unsecured = unsecuredAccessAllowed == "PublicNetwork"
	//}

	//delete(ns.Settings, "PublicServerUrl")
	//delete(ns.Settings, "PublicServerUrl.Tcp")
	//delete(ns.Settings, "Security.UnsecuredAccessAllowed")

	ns.DatabasesToDelete = []DatabaseToDelete{}
	if sc.DatabasesToDelete != nil {
		for _, database := range sc.DatabasesToDelete {
			ns.DatabasesToDelete = append(ns.DatabasesToDelete, DatabaseToDelete{
				Name:       database.Name,
				HardDelete: database.HardDelete,
			})
		}
	}

	ns.IndexesToDelete = []IndexesToDelete{}
	if sc.IndexesToDelete != nil {
		for _, index := range sc.IndexesToDelete {
			ns.IndexesToDelete = append(ns.IndexesToDelete, IndexesToDelete{
				DatabaseName: index.DatabaseName,
				IndexesNames: index.IndexesNames,
			})
		}
	}

	ns.Databases = []Database{}
	if sc.Databases != nil {
		for _, database := range sc.Databases {
			ns.Databases = append(ns.Databases, Database{
				Name:             database.Name,
				Key:              database.Key,
				Settings:         database.Settings,
				ReplicationNodes: database.ReplicationNodes,
				Indexes:          database.Indexes,
			})
		}
	}

	return ns, nil
}

func (sc *ServerConfig) deployServer(publicIP string, index int) (err error) {
	var stdoutBuf bytes.Buffer
	var conn *ssh.Client
	var settings map[string]interface{}

	idx := string(rune(index + 'A'))

	defer func() {
		log.Println(stdoutBuf.String())
	}()
	ravenPackageUrl := "https://daily-builds.s3.us-east-1.amazonaws.com/ravendb_" + sc.Package.Version + sc.Package.Arch

	signer, err := ssh.ParsePrivateKey(sc.SSH.Pem)
	if err != nil {
		return err
	}
	authConfig := &ssh.ClientConfig{
		User:            sc.SSH.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         1 * time.Minute,
	}
	conn, err = sc.ConnectToRemoteWithRetry(publicIP, conn, authConfig)
	if err != nil {
		return err
	}
	defer conn.Close()
	err = sc.execute(publicIP, []string{
		"n=0; while [ \"$n\" -lt 20 ] && [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; n=$(( n + 1 )); sleep 1; done",
		"wget -nv -O ravendb.deb " + ravenPackageUrl,
		"timeout 100 bash -c -- 'while ! sudo apt-get update -y; do sleep 1; done'",
		"sudo apt-get install -y -f ./ravendb.deb",
	}, "", &stdoutBuf, conn)
	if err != nil {
		return err
	}

	scheme := "https"
	if sc.Unsecured {
		settings["Security.UnsecuredAccessAllowed"] = "PublicNetwork"
		scheme = "http"
	}

	httpUrl, err := sc.setupUrls(index, scheme, &settings)
	if err != nil {
		return err
	}

	err = sc.ExtractSettingsAfterReadingZipPackage(index, &settings)
	if err != nil {
		return err
	}

	jsonOut, err := json.MarshalIndent(settings, "", "\t")
	if err != nil {
		return err
	}

	if rootFolderItems, ok := sc.ClusterSetupZip[CREDENTIALS_FOR_SECURE_STORE_FIELD_NAME]; ok {
		err = upload(conn, stdoutBuf, "/etc/ravendb/license.json", rootFolderItems.License)
		if err != nil {
			return err
		}
	}

	if nodeSetupContent, ok := sc.ClusterSetupZip[idx]; ok {
		err = upload(conn, stdoutBuf, "/etc/ravendb/settings.json", jsonOut)
		if err != nil {
			return err
		}

		if sc.Unsecured == false {
			err = upload(conn, stdoutBuf, "/etc/ravendb/security/server.pfx", nodeSetupContent.Pfx)
			if err != nil {
				return err
			}
		}

		err = sc.execute(publicIP, []string{
			"sudo chown ravendb:ravendb /etc/ravendb/security/server.pfx",
		}, "sudo systemctl status ravendb", &stdoutBuf, conn)
		if err != nil {
			return err
		}

	}

	for path, content := range sc.Assets {
		splittedPath := strings.Split(path, "/")
		directories := splittedPath[1 : len(splittedPath)-1]
		absolutePath := strings.Join(directories, "/")

		err = sc.execute(publicIP, []string{
			"sudo mkdir -p /" + absolutePath,
		}, "", &stdoutBuf, conn)
		if err != nil {
			return err
		}

		err = upload(conn, stdoutBuf, path, content)
		if err != nil {
			return err
		}
	}

	err = GetDnsLookup(httpUrl, sc.Hosts[index])
	if err != nil {
		return err
	}

	err = sc.execute(publicIP, []string{
		"sudo chown ravendb:ravendb /etc/ravendb/license.json",
		"sudo systemctl restart ravendb",
		"timeout 100 bash -c -- 'while ! curl -vvv -k " + httpUrl + "/setup/alive; do echo \"Curl failed with exit code $?\"; sleep 1; done'",
	}, "sudo systemctl status ravendb", &stdoutBuf, conn)
	if err != nil {
		return err
	}

	return nil
}

func GetDnsLookup(httpUrl string, hostIp string) error {
	isUpdatedDns := false
	var ipsStringSlice []string

	url, err := url.Parse(httpUrl)
	if err != nil {
		panic(err)
	}

	ips, err := net.LookupIP(url.Host)
	if err != nil {
		return err
	}

	for _, ip := range ips {
		ipsStringSlice = append(ipsStringSlice, ip.String())
		if hostIp == ip.String() {
			isUpdatedDns = true
		}
	}

	if isUpdatedDns == false {
		rowsOfIps := strings.Join(ipsStringSlice, "\n")
		return errors.New("Tried to resolve '" + url.Host + "'but got an outdated result" + "\nExpected to get these ips: " + hostIp + " while the actual result was: " + rowsOfIps)
	}

	return nil
}

func (sc *ServerConfig) setupUrls(index int, scheme string, settings *map[string]interface{}) (string, error) {
	err := json.Unmarshal(sc.ClusterSetupZip[string(rune(index+'A'))].SettingsJson, &settings)
	if err != nil {
		return "", nil
	}

	if scheme == "https" {
		if val, ok := (*settings)["PublicServerUrl"]; ok {
			sc.Url.List[index] = val.(string)
		} else {
			return "", errors.New("'PublicServerUrl' setting was not found  in 'settings.json' file\nPlease verify ZIP file integrity")
		}
	} else {
		if val, ok := (*settings)["ServerUrl"]; ok {
			sc.Url.List[index] = val.(string)
		} else {
			return "", errors.New("'ServerUrl' setting was not found  in 'settings.json' file\nPlease verify ZIP file integrity")
		}
	}

	httpUrl, tcpUrl, err := sc.GetUrlByIndex(index, scheme)
	if err != nil {
		return "", err
	}
	if scheme == "https" {
		(*settings)["PublicServerUrl"] = httpUrl
		(*settings)["PublicServerUrl.Tcp"] = tcpUrl
	}
	return httpUrl, nil
}
func (sc *ServerConfig) GetUrlByIndex(index int, scheme string) (string, string, error) {
	if sc.Url.HttpPort == 0 {
		if sc.Unsecured == false {
			sc.Url.HttpPort = DEFAULT_SECURE_RAVENDB_HTTP_PORT
		} else {
			sc.Url.HttpPort = DEFAULT_USECURED_RAVENDB_HTTP_PORT
		}
	}
	if sc.Url.TcpPort == 0 {
		if sc.Unsecured == false {
			sc.Url.HttpPort = DEFAULT_SECURE_RAVENDB_TCP_PORT
		} else {
			sc.Url.TcpPort = DEFAULT_UNSECURED_RAVENDB_TCP_PORT
		}
	}

	u, err := url.Parse(sc.Url.List[index])
	if err != nil {
		return "", "", err
	}
	host := sc.maybeAddHttpPortToHost(u.Hostname())
	httpUrl := url.URL{
		Host:   host,
		Scheme: scheme,
	}
	tcpUrl := url.URL{
		Host:   u.Hostname() + ":" + strconv.Itoa(sc.Url.TcpPort),
		Scheme: "tcp",
	}
	return httpUrl.String(), tcpUrl.String(), nil
}

func (sc *ServerConfig) maybeAddHttpPortToHost(host string) string {
	if sc.Unsecured == true && sc.Url.HttpPort != DEFAULT_HTTP_PORT || sc.Unsecured == false && sc.Url.HttpPort != DEFAULT_SECURE_RAVENDB_HTTP_PORT {
		host += ":" + strconv.Itoa(sc.Url.HttpPort)
	}
	return host
}

func (sc *ServerConfig) ExtractSettingsAfterReadingZipPackage(index int, settings *map[string]interface{}) error {
	err := json.Unmarshal(sc.ClusterSetupZip[string(rune(index+'A'))].SettingsJson, &settings)
	if err != nil {
		return nil
	}

	scheme := "https"
	if sc.Unsecured == false {
		(*settings)["ServerUrl"] = scheme + "://0.0.0.0:" + strconv.Itoa(sc.Url.HttpPort)
		(*settings)["ServerUrl.Tcp"] = "tcp://0.0.0.0:" + strconv.Itoa(sc.Url.TcpPort)
		(*settings)["License.Path"] = "/etc/ravendb/license.json"
		(*settings)["Security.Certificate.Path"] = "/etc/ravendb/security/server.pfx"
		(*settings)["License.Eula.Accepted"] = true
		(*settings)["Setup.Mode"] = "None"
	} else {
		scheme = "http"
		(*settings)["ServerUrl"] = scheme + "://0.0.0.0:" + strconv.Itoa(sc.Url.HttpPort)
		(*settings)["ServerUrl.Tcp"] = "tcp://0.0.0.0:" + strconv.Itoa(sc.Url.TcpPort)
		(*settings)["License.Path"] = "/etc/ravendb/license.json"
		(*settings)["Setup.Mode"] = "None"
	}
	return nil
}

func (sc *ServerConfig) ConvertPfx() (holder CertificateHolder, err error) {
	if sc.ClusterSetupZip == nil || sc.Unsecured {
		return holder, nil
	}
	var stdoutBuf bytes.Buffer
	var conn *ssh.Client
	defer func() {
		log.Println(stdoutBuf.String())
	}()

	signer, err := ssh.ParsePrivateKey(sc.SSH.Pem)
	if err != nil {
		return holder, err
	}
	authConfig := &ssh.ClientConfig{
		User:            sc.SSH.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         1 * time.Minute,
	}
	conn, err = sc.ConnectToRemoteWithRetry(sc.Hosts[0], conn, authConfig)
	if err != nil {
		return holder, err
	}
	defer conn.Close()

	return sc.extractServerKeyAndCertForStore(sc.Hosts[0], conn, stdoutBuf)
}

func (sc *ServerConfig) copyToRemoteGivenAbsolutePath(publicIP string, path string, stdoutBuf bytes.Buffer, conn *ssh.Client, content []byte) error {
	splittedPath := strings.Split(path, "/")
	directories := splittedPath[1 : len(splittedPath)-1]
	absolutePath := strings.Join(directories, "/")

	err := sc.execute(publicIP, []string{
		"sudo mkdir -p /" + absolutePath,
	}, "", &stdoutBuf, conn)
	if err != nil {
		return err
	}

	err = upload(conn, stdoutBuf, path, content)
	if err != nil {
		return err
	}
	return nil
}

func (sc *ServerConfig) extractServerKeyAndCertForStore(publicIP string, conn *ssh.Client, stdoutBuf bytes.Buffer) (CertificateHolder, error) {
	var certHolder CertificateHolder
	var pfx = "/etc/ravendb/security/server.pfx"
	var key = "/etc/ravendb/security/server.key"
	var crt = "/etc/ravendb/security/server.crt"

	err := sc.execute(publicIP, []string{
		"sudo openssl pkcs12 -in " + pfx + " -nocerts -nodes -out " + key + " -password pass:",
		"sudo openssl pkcs12 -in " + pfx + " -clcerts -nokeys -out " + crt + " -password pass:",
	}, "", &stdoutBuf, conn)
	if err != nil {
		return CertificateHolder{}, err
	}

	bytes, err := readFileContents(key, stdoutBuf, conn)
	if err != nil {
		return CertificateHolder{}, err
	}
	certHolder.Key = bytes

	bytes, err = readFileContents(crt, stdoutBuf, conn)
	if err != nil {
		return CertificateHolder{}, err
	}
	certHolder.Cert = bytes

	err = sc.execute(publicIP, []string{
		"sudo rm " + key + "\n",
		"sudo rm " + crt + "\n",
	}, "", &stdoutBuf, conn)
	if err != nil {
		return CertificateHolder{}, err
	}

	return certHolder, nil
}

func (sc *ServerConfig) ConnectToRemoteWithRetry(publicIP string, conn *ssh.Client, authConfig *ssh.ClientConfig) (*ssh.Client, error) {
	var err error
	hostAndPort := net.JoinHostPort(publicIP, fmt.Sprint(sc.SSH.getPort()))
	log.Println("Trying to SHH: " + hostAndPort)
	for i := 0; i <= NUMBER_OF_RETRIES; i++ {
		conn, err = ssh.Dial("tcp", hostAndPort, authConfig)
		if err != nil && i < NUMBER_OF_RETRIES {
			time.Sleep(time.Second * 2)
		} else if err == nil {
			log.Println("Connected to " + hostAndPort)
			break
		} else {
			log.Println("Unable to SSH to " + hostAndPort + " because " + err.Error())
			return nil, errors.New("Unable to SSH to " + hostAndPort + " because " + err.Error())
		}
	}
	return conn, nil
}

type debugWriter struct {
	mu        sync.Mutex
	stdoutBuf *bytes.Buffer
}

func (w *debugWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stdoutBuf.Write(p)
	return len(p), nil
}

func (sc *ServerConfig) execute(publicIp string, commands []string, onErr string, stdoutBuf *bytes.Buffer, conn *ssh.Client) error {
	writer := debugWriter{
		stdoutBuf: stdoutBuf,
	}
	stdoutBuf.WriteString(publicIp)
	for _, cmd := range commands {
		cmdStr := "$ " + cmd + "\n"
		stdoutBuf.WriteString(cmdStr)
		writer.Write([]byte(cmdStr))
		session, err := conn.NewSession()
		if err != nil {
			return err
		}

		session.Stdout = &writer
		session.Stderr = &writer

		err = session.Run(cmd)
		if err != nil {
			log.Println(err)
			if onErr != "" {
				session.Run(cmd) // executed to write to the log
			}
			session.Close()

			return &DeployError{
				Err:    err,
				Output: stdoutBuf.String(),
			}
		}
		session.Close()
	}
	return nil
}

func (sc *ServerConfig) Deploy(parallel bool) (string, error) {
	var databaseDoesNotExistError *ravendb.DatabaseDoesNotExistError
	var err error

	//err = sc.deployRavenDbInstances(parallel)
	//if err != nil {
	//	return "", err
	//}

	store, err := getStore(sc, *sc.ClusterSetupZip[CREDENTIALS_FOR_SECURE_STORE_FIELD_NAME])
	if err != nil {
		return "", err
	}

	clusterTopology, err := sc.getClusterTopology(store)
	if err != nil {
		return "", err
	}

	err = sc.getDatabaseHealthCheck(store)
	if errors.As(err, &databaseDoesNotExistError) {
		err = sc.createDatabase(store, &ravendb.DatabaseRecord{
			DatabaseName: sc.HealthcheckDatabase,
			Encrypted:    false,
		}, len(sc.Hosts))
		if err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}

	err = sc.deleteDatabases(store)
	if err != nil {
		return "", err
	}

	err = sc.createDatabases(store)
	if err != nil {
		return "", err
	}

	err = sc.deleteIndexes(store)
	if err != nil {
		return "", err
	}

	err = sc.createIndexes(store)
	if err != nil {
		return "", err
	}

	defer store.Close()

	return clusterTopology.Topology.TopologyID, nil
}

func (sc *ServerConfig) createDatabases(store *ravendb.DocumentStore) error {
	var err error
	var databaseRecords []ravendb.DatabaseRecord

	for _, database := range sc.Databases {
		databaseRecord := &ravendb.DatabaseRecord{
			DatabaseName: database.Name,
			Settings:     database.Settings,
			Encrypted:    true,
			DatabaseTopology: &ravendb.DatabaseTopology{
				Members:                  database.ReplicationNodes,
				ReplicationFactor:        len(database.ReplicationNodes),
				DynamicNodesDistribution: false,
			},
		}
		databaseRecords = append(databaseRecords, *databaseRecord)
	}

	for _, database := range sc.Databases {
		if len(strings.TrimSpace(database.Key)) > 0 {
			err = sc.distributeSecretKey(store, database)
			if err != nil {
				log.Println("Unable to DISTRIBUTE database key to nodes: " + cast.ToString(database.ReplicationNodes) + " because " + err.Error())
				return err
			}
		}
	}

	for index, database := range sc.Databases {
		databaseRecord := &databaseRecords[index]
		if len(strings.TrimSpace(database.Key)) > 0 {
			err = sc.createDatabase(store, databaseRecord, 0)
			if err != nil && strings.Contains(err.Error(), "already exists!") {
				err = sc.modifyDatabase(store, databaseRecord)
				if err != nil {
					log.Println("Unable to MODIFY database: " + databaseRecord.DatabaseName + " because: " + err.Error())
					return err
				}
			} else {
				log.Println("Unable to CREATE database: " + databaseRecord.DatabaseName + " because: " + err.Error())
				return err
			}
		} else {
			databaseRecord.Encrypted = false
			err = sc.createDatabase(store, databaseRecord, 0)
			if err != nil && strings.Contains(err.Error(), "already exists!") {
				err = sc.modifyDatabase(store, databaseRecord)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}
	return nil
}

func (sc *ServerConfig) getDbPermissionsAndAdminCertHolder() (CertificateHolder, map[string]string, error) {
	var adminCertHolder CertificateHolder
	var permissions map[string]string
	permissions = make(map[string]string)

	name := CREDENTIALS_FOR_SECURE_STORE_FIELD_NAME
	if val, ok := sc.ClusterSetupZip[name]; ok {
		adminCertHolder = *val
	} else {
		return CertificateHolder{}, nil, errors.New("cannot retrieve admin certificate from zip file. ")
	}

	if len(strings.TrimSpace(sc.HealthcheckDatabase)) != 0 {
		permissions[sc.HealthcheckDatabase] = certificates.Admin.String()
	}

	return adminCertHolder, permissions, nil
}

func (sc *ServerConfig) getDatabaseHealthCheck(store *ravendb.DocumentStore) error {
	databaseHealthCheck := operations.OperationDatabaseHealthCheck{}
	err := executeWithRetriesMaintenanceOperations(store, &databaseHealthCheck)
	if err != nil {
		return err
	}
	return nil
}

func (sc *ServerConfig) getClusterTopology(store *ravendb.DocumentStore) (operations.OperationGetClusterTopology, error) {
	clusterTopology := operations.OperationGetClusterTopology{}
	err := executeWithRetries(store, &clusterTopology)
	if err != nil {
		return operations.OperationGetClusterTopology{}, err
	}
	return clusterTopology, nil
}

func getStore(config *ServerConfig, certificateHolder CertificateHolder) (*ravendb.DocumentStore, error) {
	//var host string
	var store *ravendb.DocumentStore

	//host = config.Url.List[0]
	serverNode := []string{"https://localhost:443"}

	if len(strings.TrimSpace(config.HealthcheckDatabase)) != 0 {
		store = ravendb.NewDocumentStore(serverNode, config.HealthcheckDatabase)
	} else {
		store = ravendb.NewDocumentStore(serverNode, "")
	}

	if certificateHolder.Cert != nil {
		x509KeyPair, err := tls.X509KeyPair(certificateHolder.Cert, certificateHolder.Key)
		if err != nil {
			return nil, err
		}
		x509cert, err := x509.ParseCertificate(x509KeyPair.Certificate[0])
		if err != nil {
			return nil, err
		}
		store.TrustStore = x509cert
		store.Certificate = &x509KeyPair
	}

	if err := store.Initialize(); err != nil {
		return nil, err
	}

	return store, nil
}

func (sc *ServerConfig) addNodesToCluster(store *ravendb.DocumentStore) error {
	clusterTopology, err := sc.getClusterTopology(store)
	var errAllDown *ravendb.AllTopologyNodesDownError
	if errors.As(err, &errAllDown) || clusterTopology.CurrentState == "Passive" {
		for i := 1; i < len(sc.Url.List); i++ {
			err = addNodeToCluster(store, sc.Url.List[i])
			if err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	}

	clusterTopology, err = sc.getClusterTopology(store)
	if err != nil {
		return err
	}
	for nodeTag, nodeUrl := range clusterTopology.Topology.AllNodes {
		found, _ := contains(sc.Url.List, nodeUrl)
		if found {
			continue
		} else {
			parse, err := url.Parse(nodeUrl)
			if err != nil {
				return err
			}
			hostName := strings.Split(parse.Host, ".")
			tag := hostName[0]
			match, err := regexp.MatchString("[A-Za-z]{1,4}", tag)
			if match == false {
				tag = nodeTag
			} else {
				tag = strings.ToUpper(tag)
			}
			err = executeWithRetries(store, &operations.RemoveClusterNode{
				Node: nodeUrl,
				Tag:  tag,
			})
			if err != nil {
				return err
			}
		}
	}

	for _, node := range sc.Url.List {
		if containsValue(clusterTopology.Topology.AllNodes, node) {
			continue
		} else {
			err = addNodeToCluster(store, node)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (sc *ServerConfig) purgeRavenDbInstance(publicIP string) error {
	var stdoutBuf bytes.Buffer
	signer, err := ssh.ParsePrivateKey(sc.SSH.Pem)
	if err != nil {
		return err
	}

	authConfig := &ssh.ClientConfig{
		User:            sc.SSH.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	conn, err := ssh.Dial("tcp", net.JoinHostPort(publicIP, fmt.Sprint(sc.SSH.getPort())), authConfig)
	if err != nil {
		return err
	}
	defer conn.Close()

	err = sc.execute(publicIP, []string{
		"sudo apt-get -y purge ravendb",
	}, "", &stdoutBuf, conn)

	if err != nil {
		stdoutBuf.WriteString("Failed to delete ravendb instance. Host machine ip: " + publicIP + "\n")
	} else {
		stdoutBuf.WriteString("Deleted successfully ravendb instance. Host machine ip " + publicIP + "\n")

	}
	log.Println(stdoutBuf.String())
	return err
}

func (sc *ServerConfig) RemoveRavenDbInstances() diag.Diagnostics {
	var wg sync.WaitGroup
	errorsChanel := make(chan error, len(sc.Hosts))

	for index, publicIp := range sc.Hosts {
		wg.Add(1)
		go func(copyOfIndex int, copyOfPublicIp string) {
			err := sc.purgeRavenDbInstance(copyOfPublicIp)
			if err != nil {
				errorsChanel <- err
			}
			wg.Done()
		}(index, publicIp)
	}

	wg.Wait()
	close(errorsChanel)

	var result error

	for err := range errorsChanel {
		result = multierror.Append(result, err)
	}
	if result != nil {
		return diag.FromErr(fmt.Errorf(errorDelete, result.Error()))
	} else {
		return nil
	}

}

func (sc *ServerConfig) createDatabase(store *ravendb.DocumentStore, dbRecord *ravendb.DatabaseRecord, repFactor int) error {
	for i := 0; i < NUMBER_OF_RETRIES; i++ {
		topology, err := sc.getClusterTopology(store)
		if err != nil {
			return err
		}
		if len(topology.Topology.Members) != len(sc.Url.List) {
			time.Sleep(time.Second * 5)
		} else {
			break
		}
	}
	operation := ravendb.NewCreateDatabaseOperation(dbRecord, repFactor)
	err := executeWithRetries(store, operation)
	if err != nil && strings.Contains(err.Error(), "already exists!") {
		return err
	} else if err != nil && reflect.TypeOf(err) != reflect.TypeOf(&ravendb.ConcurrencyError{}) {
		return err
	}

	return nil
}

func (sc *ServerConfig) modifyDatabase(store *ravendb.DocumentStore, dbRecord *ravendb.DatabaseRecord) error {
	var rmDbInTags, members []string
	command := ravendb.NewGetDatabaseTopologyCommand()
	err := store.GetRequestExecutor(dbRecord.DatabaseName).ExecuteCommand(command, nil)
	result := command.Result
	if result != nil {
		members = dbRecord.DatabaseTopology.Members
		for _, serverNode := range result.Nodes {
			tag := serverNode.ClusterTag
			found, index := contains(members, tag)
			if found == false {
				rmDbInTags = append(rmDbInTags, tag)
			} else {
				members = append(members[:index], members[index+1:]...)
			}
		}
		if rmDbInTags != nil {
			operation := ravendb.NewDeleteDatabasesOperationWithParameters(&ravendb.DeleteDatabaseParameters{
				DatabaseNames:             []string{dbRecord.DatabaseName},
				HardDelete:                true,
				FromNodes:                 rmDbInTags,
				TimeToWaitForConfirmation: nil,
			})
			err = executeWithRetries(store, operation)
			if err != nil {
				return err
			}
		}
		err = sc.addDatabaseNode(store, dbRecord.DatabaseName, members)
		if err != nil {
			return err
		}
	} else {
		return errors.New("Unable to retrieve topology for database: " + dbRecord.DatabaseName)
	}
	return nil
}

func (sc *ServerConfig) createIndexes(store *ravendb.DocumentStore) error {
	indexDefinition := ravendb.NewIndexDefinition()
	for _, database := range sc.Databases {
		for _, idx := range database.Indexes {
			indexDefinition.Name = idx.IndexName
			for _, m := range idx.Maps {
				indexDefinition.Maps = append(indexDefinition.Maps, m)
			}
			indexDefinition.Reduce = &idx.Reduce
			indexDefinition.Configuration = idx.Configuration
			op := ravendb.NewPutIndexesOperation(indexDefinition)
			err := store.Maintenance().ForDatabase(database.Name).Send(op)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (sc *ServerConfig) deleteDatabases(store *ravendb.DocumentStore) error {
	for _, db := range sc.DatabasesToDelete {
		err := deleteDatabase(store, db)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sc *ServerConfig) deleteIndexes(store *ravendb.DocumentStore) error {
	for _, indexStruct := range sc.IndexesToDelete {
		for _, indexName := range indexStruct.IndexesNames {
			operation := ravendb.NewDeleteIndexOperation(indexName)
			err := store.Maintenance().ForDatabase(indexStruct.DatabaseName).Send(operation)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (sc *ServerConfig) addDatabaseNode(store *ravendb.DocumentStore, databaseName string, nodes []string) error {
	for _, node := range nodes {
		operation := operations.OperationAddDatabaseNode{
			Name: databaseName,
			Node: node,
		}
		err := executeWithRetries(store, &operation)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sc *ServerConfig) distributeSecretKey(store *ravendb.DocumentStore, database Database) error {
	operation := internal_operations.OperationDistributeSecretKey{
		Name:  database.Name,
		Nodes: database.ReplicationNodes,
		Key:   database.Key,
	}
	err := executeWithRetries(store, &operation)
	if err != nil {
		return err
	}
	return nil
}

func deleteDatabase(store *ravendb.DocumentStore, database DatabaseToDelete) error {
	createDatabaseOperation := ravendb.NewDeleteDatabasesOperation(database.Name, database.HardDelete)
	err := store.Maintenance().Server().Send(createDatabaseOperation)
	if err != nil {
		return err
	}
	return nil
}

func putCertificateInCluster(store *ravendb.DocumentStore, certificateName string, certificateBytes []byte, securityClearance string, permissions map[string]string) error {
	return executeWithRetries(store, &certificates.OperationPutCertificate{
		CertName:          certificateName,
		CertBytes:         certificateBytes,
		SecurityClearance: securityClearance,
		Permissions:       permissions,
	})
}

func addNodeToCluster(store *ravendb.DocumentStore, node string) error {
	parse, err := url.Parse(node)
	if err != nil {
		return err
	}
	hostName := strings.Split(parse.Host, ".")
	tag := hostName[0]
	match, err := regexp.MatchString("[A-Za-z]{1,4}", tag)
	if err != nil {
		return err
	}
	if !match {
		tag = ""
	} else {
		tag = strings.ToUpper(tag)
	}
	return executeWithRetries(store, &operations.OperationAddClusterNode{
		Url: node,
		Tag: tag,
	})

}

func executeWithRetriesMaintenanceOperations(store *ravendb.DocumentStore, operation ravendb.IVoidMaintenanceOperation) error {
	var err error
	for i := 0; i < NUMBER_OF_RETRIES; i++ {
		err = store.Maintenance().Send(operation)
		if err == nil {
			return nil
		}
		// we may need to wait a bit because adding a node to the cluster may move things around
		time.Sleep(time.Second * 5)
	}
	return err
}

func executeWithRetries(store *ravendb.DocumentStore, operation ravendb.IServerOperation) error {
	var errNoLeader *ravendb.NoLeaderError
	var err error
	for i := 0; i < NUMBER_OF_RETRIES; i++ {
		err = store.Maintenance().Server().Send(operation)
		if err == nil {
			return nil
		}
		if !errors.As(err, &errNoLeader) && !errors.As(err, &errNoLeader) {
			return err
		}
		// we may need to wait a bit because adding a node to the cluster may move things around
		time.Sleep(time.Second * 5)
	}
	return err
}

func readFileContents(path string, stdoutBuf bytes.Buffer, conn *ssh.Client) ([]byte, error) {
	session, err := conn.NewSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()
	stdoutBuf.WriteString("sudo cat " + path + "\n")
	out, err := session.CombinedOutput("sudo cat " + path)
	if err != nil {
		stdoutBuf.Write(out)
		return nil, &DeployError{
			Err:    err,
			Output: stdoutBuf.String(),
		}
	}
	return out, nil
}

func contains(s []string, str string) (bool, int) {
	for i, v := range s {
		if strings.ToUpper(v) == strings.ToUpper(str) {
			return true, i
		}
	}
	return false, -1
}

func containsValue(m map[string]string, v string) bool {
	for _, x := range m {
		if x == v {
			return true
		}
	}
	return false
}
