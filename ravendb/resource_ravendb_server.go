package ravendb

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/spf13/cast"
	"golang.org/x/crypto/argon2"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	errorCreate = "error while creating RavenDB instances: %s\n"
	errorRead   = "error reading RavenDB configuration information: %s\n"
	errorDelete = "error deleting RavenDB instances: %s\n"
)

var packageArchitectures = map[string]string{
	"arm64": "_linux-arm64",
	"arm32": "-0_armhf.deb",
	"amd64": "-0_amd64.deb",
}

func resourceRavendbServer() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceServerCreate,
		ReadContext:   resourceServerRead,
		UpdateContext: resourceServerUpdate,
		DeleteContext: resourceServerDelete,

		Schema: map[string]*schema.Schema{
			"hosts": {
				Type:        schema.TypeList,
				Required:    true,
				Description: "The hostnames (or ip addresses) of the nodes that terraform will use to setup the RavenDB cluster",
				Elem: &schema.Schema{
					Type:         schema.TypeString,
					ValidateFunc: validation.IsIPAddress,
				},
				MinItems: 1,
			},
			"database": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The database name to check whether he is alive or not",
			},
			"setup_package_zip": {
				Type:         schema.TypeString,
				Optional:     true,
				Description:  "This zip file path generated from either RavenDB setup wizard or from RavenDB RVN tool",
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"package": {
				Type:        schema.TypeSet,
				Required:    true,
				MaxItems:    1,
				Description: "The RavenDB download package set",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"version": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "The RavenDB version to use for the cluster",
						},
						"arch": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: "Operating system architecture name - amd64, arm64, arm32",
						},
					},
				},
			},
			"http_port": {
				Type:         schema.TypeInt,
				Optional:     true,
				ValidateFunc: validation.IsPortNumber,
			},
			"tcp_port": {
				Type:         schema.TypeInt,
				Optional:     true,
				ValidateFunc: validation.IsPortNumber,
			},
			"settings_override": {
				Type:        schema.TypeMap,
				Optional:    true,
				Description: "Overriding the settings.json file",
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"assets": {
				Type:        schema.TypeMap,
				Optional:    true,
				Description: "Upload files to an absolute path",
				Elem: &schema.Schema{
					Type:         schema.TypeString,
					ValidateFunc: validation.StringIsBase64,
				},
			},
			"ssh": {
				Type:        schema.TypeSet,
				Required:    true,
				MaxItems:    1,
				Description: "Connection credentials needed to SSH to the machines",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"user": {
							Type:      schema.TypeString,
							Sensitive: true,
							Required:  true,
						},
						"pem": {
							Type:         schema.TypeString,
							Sensitive:    true,
							Required:     true,
							ValidateFunc: validation.StringIsBase64,
						},
					},
				},
			},
			"indexes_to_delete": {
				Type:        schema.TypeSet,
				Optional:    true,
				Description: "Indexes that will be deleted on a given database",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"index": {
							Type:     schema.TypeList,
							Required: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"database_name": {
										Type:     schema.TypeString,
										Required: true,
									},
									"indexes_names": {
										Type:     schema.TypeList,
										Required: true,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
								},
							},
						},
					},
				},
			},
			"databases_to_delete": {
				Type:        schema.TypeSet,
				Optional:    true,
				MaxItems:    1,
				Description: "Databases that will be hard/soft deleted",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"database": {
							Type:     schema.TypeList,
							Required: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": {
										Type:     schema.TypeString,
										Required: true,
									},
									"hard_delete": {
										Type:     schema.TypeBool,
										Optional: true,
									},
								},
							},
						},
					},
				},
			},
			"databases": {
				Type:        schema.TypeSet,
				Optional:    true,
				Description: "Creation of databases and indexes",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"database": {
							Type:     schema.TypeList,
							Required: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": {
										Type:     schema.TypeString,
										Required: true,
									},
									"encryption_key": {
										Type:         schema.TypeString,
										Description:  "Encryption key for the database",
										Optional:     true,
										Sensitive:    true,
										ValidateFunc: validation.StringIsBase64,
									},
									"settings": {
										Type:        schema.TypeMap,
										Optional:    true,
										Description: "Database Settings",
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
									"replication_nodes": {
										Type:        schema.TypeList,
										Description: "The database will be created on these nodes",
										Optional:    true,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
									"indexes": {
										Type:     schema.TypeList,
										Optional: true,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"index": {
													Type:     schema.TypeSet,
													Required: true,
													Elem: &schema.Resource{
														Schema: map[string]*schema.Schema{
															"index_name": {
																Type:     schema.TypeString,
																Required: true,
															},
															"maps": {
																Type:     schema.TypeList,
																Required: true,
																Elem: &schema.Schema{
																	Type: schema.TypeString,
																},
															},
															"reduce": {
																Required: true,
																Type:     schema.TypeString,
															},
															"configuration": {
																Type:     schema.TypeMap,
																Optional: true,
																Elem: &schema.Schema{
																	Type: schema.TypeString,
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			"nodes": {
				Type:        schema.TypeList,
				Computed:    true,
				Description: "The state of all the nodes in the RavenDB cluster.",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"host": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"license": {
							Type:      schema.TypeString,
							Sensitive: true,
							Computed:  true,
						},
						"version": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"settings": {
							Type:     schema.TypeMap,
							Computed: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"certificate_holder": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"http_url": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"tcp_url": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"assets": {
							Type:     schema.TypeMap,
							Computed: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"unsecured": {
							Type:     schema.TypeBool,
							Computed: true,
						},
						"failed": {
							Type:     schema.TypeBool,
							Computed: true,
						},
						"indexes_to_delete": {
							Type:     schema.TypeSet,
							Computed: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"index": {
										Type:     schema.TypeList,
										Computed: true,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"database_name": {
													Type:     schema.TypeString,
													Computed: true,
												},
												"indexes_names": {
													Type:     schema.TypeList,
													Computed: true,
													Elem: &schema.Schema{
														Type: schema.TypeString,
													},
												},
											},
										},
									},
								},
							},
						},
						"databases_to_delete": {
							Type:     schema.TypeSet,
							Computed: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"database": {
										Type:     schema.TypeList,
										Computed: true,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"name": {
													Type:     schema.TypeString,
													Computed: true,
												},
												"hard_delete": {
													Type:     schema.TypeBool,
													Computed: true,
												},
											},
										},
									},
								},
							},
						},
						"databases": {
							Type:     schema.TypeSet,
							Computed: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"database": {
										Type:     schema.TypeList,
										Computed: true,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"name": {
													Type:     schema.TypeString,
													Computed: true,
												},
												"encryption_key": {
													Type:     schema.TypeString,
													Computed: true,
												},
												"settings": {
													Type:     schema.TypeMap,
													Computed: true,
													Elem: &schema.Schema{
														Type: schema.TypeString,
													},
												},
												"replication_nodes": {
													Type:     schema.TypeList,
													Computed: true,
													Elem: &schema.Schema{
														Type: schema.TypeString,
													},
												},
												"indexes": {
													Type:     schema.TypeList,
													Computed: true,
													Elem: &schema.Resource{
														Schema: map[string]*schema.Schema{
															"index": {
																Type:     schema.TypeSet,
																Computed: true,
																Elem: &schema.Resource{
																	Schema: map[string]*schema.Schema{
																		"index_name": {
																			Type:     schema.TypeString,
																			Computed: true,
																		},
																		"maps": {
																			Type:     schema.TypeList,
																			Computed: true,
																			Elem: &schema.Schema{
																				Type: schema.TypeString,
																			},
																		},
																		"reduce": {
																			Computed: true,
																			Type:     schema.TypeString,
																		},
																		"configuration": {
																			Type:     schema.TypeMap,
																			Computed: true,
																			Elem: &schema.Schema{
																				Type: schema.TypeString,
																			},
																		},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
func resourceServerCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	sc, err := parseData(d)
	if err != nil {
		return diag.FromErr(fmt.Errorf(errorCreate, err.Error()))
	}

	id, err := sc.Deploy(true)
	if err != nil {
		return diag.FromErr(fmt.Errorf(errorCreate, err.Error()))
	}
	d.SetId(id)

	return resourceServerRead(ctx, d, meta)
}

func resourceServerDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	sc, err := parseData(d)
	if err != nil {
		return diag.FromErr(fmt.Errorf(errorDelete, err.Error()))
	}
	return sc.RemoveRavenDbInstances()
}

func convertNode(node NodeState, index int) (map[string]interface{}, error) {
	idx := string(rune(index + 'A'))

	convertCert, err := node.ClusterSetupZip[idx].String()
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"host":                node.Host,
		"license":             base64.StdEncoding.EncodeToString(node.Licence),
		"settings":            node.Settings,
		"certificate_holder":  convertCert,
		"databases":           flattenDatabases(node.Databases),
		"databases_to_delete": flattenDatabasesToDelete(node.DatabasesToDelete),
		"indexes_to_delete":   flattenIndexesToDelete(node.IndexesToDelete),
		"assets":              node.Assets,
		"unsecured":           node.Unsecured,
		"version":             node.Version,
		"failed":              node.Failed,
	}, nil
}
func flattenIndexesToDelete(indexesToDelete []IndexesToDelete) []map[string]interface{} {
	tfs := make([]map[string]interface{}, 0)
	for _, v := range indexesToDelete {
		tf := map[string]interface{}{
			"index": []map[string]interface{}{
				{
					"database_name": v.DatabaseName,
					"indexes_names": v.IndexesNames,
				},
			},
		}
		tfs = append(tfs, tf)
	}
	return tfs
}

func (sc CertificateHolder) String() (string, error) {
	out, err := json.MarshalIndent(sc, "", "\t")
	if err != nil {
		return "", nil
	}
	return string(out), nil
}

func parseData(d *schema.ResourceData) (ServerConfig, error) {
	var sc ServerConfig
	var err error
	var getUrlFromSettingsJson map[string]interface{}

	hosts := d.Get("hosts").([]interface{})

	if sc.Hosts == nil {
		sc.Hosts = make([]string, len(hosts))
	}

	for i, host := range hosts {
		sc.Hosts[i] = host.(string)
	}

	if dbName, ok := d.GetOk("database"); ok {
		sc.HealthcheckDatabase = dbName.(string)
	}

	if httpPort, ok := d.GetOk("http_port"); ok {
		sc.Url.HttpPort = httpPort.(int)
	}

	if tcpPort, ok := d.GetOk("tcp_port"); ok {
		sc.Url.TcpPort = tcpPort.(int)
	}

	if zipPath, ok := d.GetOk("setup_package_zip"); ok {
		sc.ClusterSetupZip, err = OpenZipFile(sc, zipPath.(string))
		if err != nil {
			return sc, err
		}
		if rootSecuredPackage, _ := sc.ClusterSetupZip[CREDENTIALS_FOR_SECURE_STORE_FIELD_NAME]; ok {
			if len(rootSecuredPackage.Pfx) != 0 {
				sc.Unsecured = false
			} else {
				sc.Unsecured = true
			}
		}
	} else {
		return sc, errors.New("setup_package_zip path must be set in order to continue setup")
	}

	if sc.Url.List == nil {
		sc.Url.List = make([]string, len(hosts))
	}
	for index := 0; index < len(sc.Hosts); index++ {
		err = sc.ExtractSettingsAfterReadingZipPackage(index, &getUrlFromSettingsJson)
		if err != nil {
			return sc, err
		}
	}

	packageSet := d.Get("package").(*schema.Set).List()
	for _, v := range packageSet {
		value := v.(map[string]interface{})
		sc.Package.Version = value["version"].(string)
		sc.Package.Arch = value["arch"].(string)
		err := validatePackage(&sc)
		if err != nil {
			return sc, err
		}
	}

	assets := d.Get("assets").(map[string]interface{})
	sc.Assets = map[string][]byte{}
	for name, base64Val := range assets {
		value, err := base64.StdEncoding.DecodeString(base64Val.(string))
		if err != nil {
			return sc, err
		}
		sc.Assets[name] = value
	}
	settings := d.Get("settings_override").(map[string]interface{})
	sc.Settings = make(map[string]interface{})
	for k, v := range settings {
		sc.Settings[k] = v.(string)
	}

	sshSet := d.Get("ssh").(*schema.Set).List()
	for _, v := range sshSet {
		value := v.(map[string]interface{})
		sc.SSH.User = value["user"].(string)
		pemBase64 := value["pem"].(string)
		pem, err := base64.StdEncoding.DecodeString(pemBase64)
		if err != nil {
			return sc, err
		}
		sc.SSH.Pem = pem
	}

	if d.HasChange("databases_to_delete") {
		databasesToDeleteList := d.Get("databases_to_delete").(*schema.Set).List()
		sc.DatabasesToDelete, err = parseDatabasesToDelete(databasesToDeleteList)
		if err != nil {
			return sc, err
		}
	}

	if d.HasChange("indexes_to_delete") {
		indexesToDeleteList := d.Get("indexes_to_delete").(*schema.Set).List()
		sc.IndexesToDelete, err = parseIndexesToDelete(indexesToDeleteList)
		if err != nil {
			return sc, err
		}
	}

	if d.HasChange("databases") {
		databasesList := d.Get("databases").(*schema.Set).List()
		sc.Databases, err = sc.parseDatabases(databasesList)
		if err != nil {
			return sc, err
		}
	}

	return sc, nil
}

func parseIndexesToDelete(indexesToDeleteList []interface{}) ([]IndexesToDelete, error) {
	var indexesToDelete []IndexesToDelete

	for _, index := range indexesToDeleteList {
		val := cast.ToStringMap(index)

		indexesSet, err := cast.ToSliceE(val["index"])
		if err != nil {
			return nil, err
		}

		for _, setVal := range indexesSet {
			index := cast.ToStringMap(setVal)
			dbName := cast.ToString(index["database_name"])
			indexesNamesSlice := cast.ToStringSlice(index["indexes_names"])
			indexesToDelete = append(indexesToDelete, IndexesToDelete{
				DatabaseName: dbName,
				IndexesNames: indexesNamesSlice,
			})
		}
	}

	return indexesToDelete, nil
}

func parseDatabasesToDelete(databasesToDeleteList []interface{}) ([]DatabaseToDelete, error) {
	var databasesToDelete []DatabaseToDelete
	for _, database := range databasesToDeleteList {
		val := cast.ToStringMap(database)

		databases, err := cast.ToSliceE(val["database"])
		if err != nil {
			return nil, err
		}

		for _, db := range databases {
			val = cast.ToStringMap(db)
			name := cast.ToString(val["name"])
			hardDelete := cast.ToBool(val["hard_delete"])

			databasesToDelete = append(databasesToDelete, DatabaseToDelete{
				Name:       name,
				HardDelete: hardDelete,
			})
		}
	}
	return databasesToDelete, nil
}

func (sc *ServerConfig) parseDatabases(databasesList []interface{}) ([]Database, error) {
	var databases []Database
	for _, v := range databasesList {
		val := cast.ToStringMap(v)
		databasesSlice, err := cast.ToSliceE(val["database"])
		if err != nil {
			return nil, err
		}

		for _, db := range databasesSlice {
			val = cast.ToStringMap(db)
			name := cast.ToString(val["name"])
			key := cast.ToString(val["encryption_key"])
			if len(strings.TrimSpace(key)) != 0 && sc.Unsecured == true {
				return nil, errors.New("encryption key can be used only in secured mode. ")
			}
			databaseSettings := cast.ToStringMapString(val["settings"])
			replicationNodes := cast.ToStringSlice(val["replication_nodes"])
			if replicationNodes == nil {
				replicationNodes = append(replicationNodes, "A")
			}

			database := Database{
				Name:             name,
				Settings:         databaseSettings,
				ReplicationNodes: replicationNodes,
				Key:              key,
			}

			indexesSlice, err := cast.ToSliceE(val["indexes"])
			if err != nil {
				return nil, err
			}

			for _, index := range indexesSlice {
				val := cast.ToStringMap(index)
				indexes := val["index"].(*schema.Set).List()
				for _, index := range indexes {
					val = cast.ToStringMap(index)
					indexName := cast.ToString(val["index_name"])
					reduce := cast.ToString(val["reduce"])
					mapsSlice := cast.ToStringSlice(val["maps"])
					configurationMap := cast.ToStringMapString(val["configuration"])

					dbIndex := Index{
						IndexName:     indexName,
						Maps:          mapsSlice,
						Reduce:        reduce,
						Configuration: configurationMap,
					}
					database.Indexes = append(database.Indexes, dbIndex)
				}
			}
			databases = append(databases, database)
		}
	}
	return databases, nil
}

func OpenZipFile(sc ServerConfig, path string) (map[string]*CertificateHolder, error) {
	var split []string

	zipReader, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	defer zipReader.Close()

	var clusterSetupZip = make(map[string]*CertificateHolder, len(sc.Hosts))

	for _, file := range zipReader.Reader.File {
		split = strings.Split(file.Name, "/")
		var name string
		if len(split) == 1 {
			name = CREDENTIALS_FOR_SECURE_STORE_FIELD_NAME
		} else {
			name = split[0]
		}
		if _, found := clusterSetupZip[name]; !found {
			clusterSetupZip[name] = &CertificateHolder{
				Pfx:          make([]byte, 0),
				Cert:         make([]byte, 0),
				Key:          make([]byte, 0),
				SettingsJson: make([]byte, 0),
			}
		}
		err = extractFiles(file, clusterSetupZip[name])
		if err != nil {
			return nil, err
		}
	}

	return clusterSetupZip, nil
}

func extractFiles(file *zip.File, certHolder *CertificateHolder) error {
	rc, err := file.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	bytes, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}

	fileExtension := filepath.Ext(file.Name)
	switch fileExtension {
	case ".pfx":
		certHolder.Pfx = bytes
	case ".crt":
		certHolder.Cert = bytes
	case ".key":
		certHolder.Key = bytes
	case ".json":
		{
			if file.Name == "license.json" {
				certHolder.License = bytes
			} else {
				certHolder.SettingsJson = bytes
			}
		}
	}
	return nil
}

func validatePackage(sc *ServerConfig) error {
	arc := strings.ToLower(sc.Package.Arch)
	if val, ok := packageArchitectures[arc]; ok {
		sc.Package.Arch = val
	}
	if len(strings.TrimSpace(sc.Package.Arch)) == 0 {
		sc.Package.Arch = "-0_amd64.deb"
	}
	link := "https://daily-builds.s3.us-east-1.amazonaws.com/ravendb_" + sc.Package.Version + sc.Package.Arch
	response, err := http.Head(link)
	if err != nil {
		return errors.New("unable to download the RavenDB version: " + sc.Package.Version + ", from: " + link + " because of:" + err.Error())
	} else if response.StatusCode != http.StatusOK {
		return errors.New(link + " is not reachable. HTTP status code: " + strconv.Itoa(response.StatusCode) + ". Please check the input version:" + sc.Package.Version + ". Url used was:" + link)
	}
	return nil
}

func resourceServerRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	sc, err := parseData(d)
	if err != nil {
		return diag.FromErr(fmt.Errorf(errorRead, err.Error()))
	}

	if sc.Unsecured == false {
		certHolder, err := sc.ConvertPfx()
		if err != nil {
			return diag.FromErr(fmt.Errorf(errorRead, err.Error()))
		}
		sc.ClusterSetupZip["A"] = &certHolder
	}

	nodes, err := readRavenDbInstances(sc)
	if err != nil {
		return diag.FromErr(fmt.Errorf(errorRead, err.Error()))
	}
	convertedNodes := make([]interface{}, len(nodes))
	for index, node := range nodes {
		if node.Failed == false {
			convertedMap, err := convertNode(node, index)
			if err != nil {
				return diag.FromErr(fmt.Errorf(errorRead, "unable to convert node. Index of node: "+strconv.Itoa(index)+err.Error()))
			}
			convertedNodes[index] = convertedMap
		}
	}

	err = d.Set("nodes", convertedNodes)
	if err != nil {
		return diag.FromErr(fmt.Errorf(errorRead, err.Error()))
	}

	return nil
}

func flattenDatabases(databases []Database) []map[string]interface{} {
	tfs := make([]map[string]interface{}, 0)
	for _, db := range databases {
		tf := map[string]interface{}{
			"database": []map[string]interface{}{
				{
					"name":              db.Name,
					"settings":          db.Settings,
					"replication_nodes": db.ReplicationNodes,
					"encryption_key":    encryptionKeyHash(db.Key),
					"indexes":           flattenIndexes(db.Indexes),
				},
			},
		}
		tfs = append(tfs, tf)
	}
	return tfs
}

func encryptionKeyHash(plainText string) string {
	argonParams := &params{
		Memory:      64 * 1024,
		Iterations:  4,
		Parallelism: 4,
		SaltLength:  16,
		KeyLength:   32,
	}
	hash, err := generateFromPlainText(plainText, argonParams)
	if err != nil {
		log.Fatal(err)
	}

	return hex.EncodeToString(hash)
}

func generateFromPlainText(plainText string, p *params) (hash []byte, err error) {
	salt, err := generateRandomBytes(p.SaltLength)
	if err != nil {
		return nil, err
	}
	hash = argon2.IDKey([]byte(plainText), salt, p.Iterations, p.Memory, p.Parallelism, p.KeyLength)

	return hash, nil
}

func generateRandomBytes(n uint32) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func flattenIndexes(indexes []Index) []map[string]interface{} {
	tfs := make([]map[string]interface{}, 0)
	for _, index := range indexes {
		tf := map[string]interface{}{
			"index": []map[string]interface{}{
				{
					"index_name":    index.IndexName,
					"maps":          index.Maps,
					"reduce":        index.Reduce,
					"configuration": index.Configuration,
				},
			},
		}
		tfs = append(tfs, tf)
	}

	return tfs
}

func flattenDatabasesToDelete(databasesToDelete []DatabaseToDelete) []map[string]interface{} {
	tfs := make([]map[string]interface{}, 0)
	for _, v := range databasesToDelete {
		tf := map[string]interface{}{
			"database": []map[string]interface{}{
				{
					"name":        v.Name,
					"hard_delete": v.HardDelete,
				},
			},
		}
		tfs = append(tfs, tf)
	}
	return tfs
}

func readRavenDbInstances(sc ServerConfig) ([]NodeState, error) {
	var wg sync.WaitGroup
	var errResults error
	errorsChanel := make(chan error, len(sc.Hosts))

	nodeStateArray := make([]NodeState, len(sc.Hosts))

	wg.Add(len(sc.Hosts))
	for index, publicIp := range sc.Hosts {
		go func(copyOfPublicIp string, copyOfIndex int) {
			nodeState, err := sc.ReadServer(copyOfPublicIp, copyOfIndex)
			if err != nil {
				if strings.Contains(err.Error(), "Unable to SSH to") {
					nodeState.Failed = true
					wg.Done()
					return
				} else {
					errorsChanel <- err
				}
			}
			wg.Done()
			nodeStateArray[copyOfIndex] = nodeState
		}(publicIp, index)
	}

	wg.Wait()
	close(errorsChanel)

	if len(errorsChanel) > 0 {
		for err := range errorsChanel {
			errResults = multierror.Append(errResults, err)
		}
		return nil, errResults
	}

	return nodeStateArray, nil
}

func resourceServerUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return resourceServerCreate(ctx, d, meta)
}
