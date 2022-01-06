package operations

import (
	"bytes"
	"encoding/json"
	"github.com/ravendb/ravendb-go-client"
	"net/http"
	"net/url"
)

type OperationDistributeSecretKey struct {
	Name  string   `json:"Name"`
	Nodes []string `json:"Nodes"`
	Key   string   `json:"Key"`
}

func (operation *OperationDistributeSecretKey) GetCommand(conventions *ravendb.DocumentConventions) (ravendb.RavenCommand, error) {
	return &operationDistributeSecretKey{
		RavenCommandBase: ravendb.RavenCommandBase{
			ResponseType: ravendb.RavenCommandResponseTypeEmpty,
		},
		parent: operation,
	}, nil
}

type operationDistributeSecretKey struct {
	ravendb.RavenCommandBase
	parent *OperationDistributeSecretKey
}

func (o *operationDistributeSecretKey) CreateRequest(node *ravendb.ServerNode) (*http.Request, error) {
	base, err := url.Parse(node.URL + "/admin/secrets/distribute")
	if err != nil {
		return nil, err
	}

	params := url.Values{}

	params.Add("name", o.parent.Name)
	for _, node := range o.parent.Nodes {
		params.Add("node", node)
	}

	base.RawQuery = params.Encode()
	body := bytes.NewBuffer([]byte(o.parent.Key))
	req, err := http.NewRequest(http.MethodPost, base.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json; charset=UTF-8")
	return req, nil
}

func (o *operationDistributeSecretKey) SetResponse(response []byte, fromCache bool) error {
	return json.Unmarshal(response, &o.parent)
}
