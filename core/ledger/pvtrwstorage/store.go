/*
Copyright IBM Corp. 2017 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pvtrwstorage

// StoreProvider provides handle to specific 'Store' that in turn manages
// private read-write sets for a namespace
type StoreProvider interface {
	OpenStore(namespace string) (Store, error)
	Close()
}

// Store manages the permanent storage of private read-write sets for a namespace
// TODO add functions for supporting pvtdata related functionality in the ledger interface
type Store interface {
}
