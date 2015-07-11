package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
)

type SchemaEntry struct {
	Keyspace       string
	ColumnFamily   string
	ColumnFamilyID string
}

func (entry *SchemaEntry) FullName() string {
	return entry.Keyspace + "." + entry.ColumnFamily
}

func (entry *SchemaEntry) String() string {
	return fmt.Sprintf("%v\t%v", entry.FullName(), entry.ColumnFamilyID)
}

type Schema []*SchemaEntry

func (a Schema) Len() int      { return len(a) }
func (a Schema) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Schema) Less(i, j int) bool {
	this := a[i]
	that := a[j]
	if this.Keyspace == that.Keyspace {
		if this.ColumnFamily == that.ColumnFamily {
			return this.ColumnFamilyID < that.ColumnFamilyID
		} else {
			return this.ColumnFamily < that.ColumnFamily
		}
	} else {
		return this.Keyspace < that.Keyspace
	}
}

func getFromSchemaFile(schemaFileName string) Schema {
	schemaFile, err := os.Open(schemaFileName)
	exitOnError(err, fmt.Sprintf("Can't open schema file %v", schemaFileName))
	defer schemaFile.Close()

	scanner := bufio.NewScanner(schemaFile)
	started := false
	schema := make([]*SchemaEntry, 0)
	for scanner.Scan() {
		line := scanner.Text()
		if !started {
			if strings.Contains(line, "-") {
				started = true
			}
		} else {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				break
			}
			fields := strings.Split(line, "|")
			schema = append(schema, &SchemaEntry{
				Keyspace:       strings.TrimSpace(fields[0]),
				ColumnFamily:   strings.TrimSpace(fields[1]),
				ColumnFamilyID: strings.Replace(strings.TrimSpace(fields[2]), "-", "", -1),
			})
		}
	}

	return schema
}

func getFromDataFolder(dataFolder string) Schema {
	keyspaceDirs, err := ioutil.ReadDir(dataFolder)
	exitOnError(err, fmt.Sprintf("Can't list directory %v", dataFolder))

	schema := make([]*SchemaEntry, 0)
	for _, keyspaceDir := range keyspaceDirs {
		keyspace := keyspaceDir.Name()
		if keyspaceDir.IsDir() && keyspace[0] != '.' {
			keyspaceFolder := dataFolder + "/" + keyspace + "/"
			columnFamilyDirs, err := ioutil.ReadDir(keyspaceFolder)
			exitOnError(err, fmt.Sprintf("Can't list directory %v", keyspaceFolder))
			for _, columnFamilyDir := range columnFamilyDirs {
				columnFamily := columnFamilyDir.Name()
				if columnFamilyDir.IsDir() && columnFamily[0] != '.' {
					i := strings.LastIndex(columnFamily, "-")
					schema = append(schema, &SchemaEntry{
						Keyspace:       keyspace,
						ColumnFamily:   columnFamily[:i],
						ColumnFamilyID: columnFamily[i+1:],
					})
				}
			}
		}
	}

	return schema
}

func getSchemaMap(schema Schema, name string) map[string]string {
	schemaMap := make(map[string]string, 0)

	for _, schemaEntry := range schema {
		oldSchemaEntry, alreadyExists := schemaMap[schemaEntry.FullName()]
		if alreadyExists {
			fmt.Printf("%v: duplicate schema entry: {%v} and {%v}", name, oldSchemaEntry, schemaEntry)
		} else {
			schemaMap[schemaEntry.FullName()] = schemaEntry.ColumnFamilyID
		}
	}

	return schemaMap
}

func main() {
	schemaFileName := os.Args[1]
	dataFolder := os.Args[2]

	force := false
	if len(os.Args) > 3 && os.Args[3] == "-f" {
		force = true
	}

	schemaFromSchemaFile := getFromSchemaFile(schemaFileName)
	schemaFromDataFolder := getFromDataFolder(dataFolder)

	sort.Sort(schemaFromSchemaFile)
	sort.Sort(schemaFromDataFolder)

	fmt.Println("From Schema File:")
	fmt.Println("-----------------")
	for _, schemaEntry := range schemaFromSchemaFile {
		fmt.Println(schemaEntry)
	}
	fmt.Println("-----------------")

	fmt.Println()

	fmt.Println("From Data Folder:")
	fmt.Println("-----------------")
	for _, schemaEntry := range schemaFromDataFolder {
		fmt.Println(schemaEntry)
	}
	fmt.Println("-----------------")

	fmt.Println()

	schemaMap := getSchemaMap(schemaFromSchemaFile, "Schema File")
	for _, schemaEntry := range schemaFromDataFolder {
		fromMap, exists := schemaMap[schemaEntry.FullName()]
		if !exists {
			fullpath := path.Join(dataFolder, schemaEntry.Keyspace, schemaEntry.ColumnFamily+"-"+schemaEntry.ColumnFamilyID)
			var answer string
			fmt.Printf("Exists in data folder, not in schema: %v.\nREMOVE %v? [Y/n] ", schemaEntry, fullpath)
			if !force {
				fmt.Scanf("%s", &answer)
			}
			if force || answer == "Y" {
				fmt.Printf("REMOVING %v ... ", fullpath)
				err := os.RemoveAll(fullpath)
				exitOnError(err, fmt.Sprintf("Can't remove %v", fullpath))
				fmt.Println("DONE.")
			}
			fmt.Println()
		} else if fromMap != schemaEntry.ColumnFamilyID {
			fromPath := path.Join(dataFolder, schemaEntry.Keyspace, schemaEntry.ColumnFamily+"-"+schemaEntry.ColumnFamilyID)
			toPath := path.Join(dataFolder, schemaEntry.Keyspace, schemaEntry.ColumnFamily+"-"+fromMap)
			fmt.Printf("%v in data folder has ID %v, but in schema it's %v\n", schemaEntry.FullName(), schemaEntry.ColumnFamilyID, fromMap)
			var answer string
			fmt.Printf("MOVE everything from %v to %v? [Y,n] ", fromPath, toPath)
			if !force {
				fmt.Scanf("%s", &answer)
			}
			if force || answer == "Y" {
				fmt.Printf("MOVING %v to %v\n", fromPath, toPath)
				snapshotsPath := path.Join(fromPath, "snapshots")
				snapshotExists, err := FilepathExists(snapshotsPath)
				exitOnError(err, fmt.Sprintf("Can't check snapshots path %v", snapshotsPath))
				if snapshotExists {
					fmt.Printf("Found old snapshots, REMOVING %v ... ", snapshotsPath)
					err := os.RemoveAll(snapshotsPath)
					exitOnError(err, fmt.Sprintf("Can't remove %v", snapshotsPath))
					fmt.Println("Done")
				}
				oldFiles, err := ioutil.ReadDir(fromPath)
				exitOnError(err, fmt.Sprintf("Can't list directory %v", fromPath))

				for _, oldFile := range oldFiles {
					oldPath := path.Join(fromPath, oldFile.Name())
					newPath := path.Join(toPath, oldFile.Name())
					newPathExists, err := FilepathExists(newPath)
					exitOnError(err, fmt.Sprintf("Can't check new path %v", newPath))

					if newPathExists {
						fmt.Printf("!!! %v already exists !!! skipping ...\n", newPath)
					} else {
						err := os.Rename(oldPath, newPath)
						exitOnError(err, fmt.Sprintf("Can't move file from %v to %v", oldPath, newPath))

					}
				}

				oldFiles, err = ioutil.ReadDir(fromPath)
				exitOnError(err, fmt.Sprintf("Can't list directory %v", fromPath))
				if len(oldFiles) == 0 {
					err := os.RemoveAll(fromPath)
					exitOnError(err, fmt.Sprintf("Can't remove %v", fromPath))
				}

				fmt.Println("RUNNING nodetool refresh", schemaEntry.Keyspace, schemaEntry.ColumnFamily)
				cmd := exec.Command("nodetool", "refresh", schemaEntry.Keyspace, schemaEntry.ColumnFamily)
				err = cmd.Run()
				exitOnError(err, "nodetool refresh failed")

				fmt.Println("DONE")
				fmt.Println()
			}
		}
	}
}

func FilepathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func exitOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%v: %v\n", msg, err)
		os.Exit(-1)
	}
}
