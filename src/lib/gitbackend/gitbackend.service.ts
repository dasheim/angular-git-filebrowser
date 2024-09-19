import { Injectable, OnDestroy } from '@angular/core';
import { AsyncSubject } from 'rxjs/AsyncSubject';
import { Observable } from 'rxjs/Observable';
import { FileBrowserService } from '../filebrowser.service';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import { FileInfo } from '../filebrowser.service';
import { mergeMap, merge, map, tap, take } from 'rxjs/operators';
import { of } from 'rxjs/observable/of';
import { fromPromise } from 'rxjs/observable/fromPromise';

/*
 * These are used in the worker - but we declare them here so that typescript doesn't complain
 */
declare var importScripts;
declare var Module;
declare var FS;
declare var IDBFS;
declare var self;

@Injectable()
export class GitBackendService extends FileBrowserService implements OnDestroy {

    workerReady: AsyncSubject<boolean> = new AsyncSubject();

    worker: Worker;
    syncInProgress = false;
    repositoryOpen = false;
    mountdir: string;
    workermessageid = 1;
    resolvemap: {[id: number]: Function} = {};
    dirlisteners: {[dir: string]: BehaviorSubject<FileInfo[]>} = {};

    constructor() {
        super();
    }

    mount(dir: string): Observable<any> {
        if (this.mountdir) {
            Observable.throw('Mount already called');
        }

        this.mountdir = dir;
        // The "stupid" worker is simply evaluating scripts that we pass on to it
        this.worker = new Worker('assets/stupid_worker.js');
        this.worker.onmessage = (msg) => {
            if (msg.data.error) {
                this.resolvemap[msg.data.id]({error: msg.data.error});
            } else {
                this.resolvemap[msg.data.id](msg.data.response);
            }
            delete this.resolvemap[msg.data.id];
        };

        // Set up emscripten with libgit2 (inside worker)
        return fromPromise(this.callWorker((params) => {

            return new Promise((resolve, reject) => {

                importScripts('libgit2.js');
                Module['onRuntimeInitialized'] = () => {

                    self.workdir = '/' + params.workdir;

                    FS.mkdir(self.workdir, '0777');
                    FS.mount(IDBFS, {}, self.workdir);
                    FS.chdir(self.workdir);

                    self.fromGitPath = (path) => {
                        const currentdir: string[] = FS.cwd().split('/');
                        const gitroot = self.workdir;
                        return gitroot + '/' + path;
                    };
                    self.toGitPath = (filename) => {
                        const currentdir: string[] = FS.cwd().substring(`${self.workdir}/`.length).split('/');
                        return currentdir.join('/') + '/' + filename;
                    };
                    self.jsgitinit();

                    resolve();
                };
            });
        }, {workdir: dir})
        ).pipe(
            mergeMap(() => this.syncLocalFS(true)),
            mergeMap(() => this.readdir()),
            mergeMap((ret) => {
                const hasGitRepo = ret.find(file => file.name === '.git');
                if (hasGitRepo) {
                    return fromPromise(
                        this.callWorker(() => {
                                console.log('Open repository');
                                self.jsgitopenrepo();
                                self.jsgitsetuser('test', 'test@example.com');
                            })
                        ).pipe(
                            tap(() => this.repositoryOpen = true),
                            mergeMap(() => this.readdir())
                        );
                } else {
                    return of(ret);
                }
            }),
            map(() => {
                this.workerReady.next(true);
                this.workerReady.complete();
            })
        );
    }

    /**
     * Convert function to string source code and send it to the worker for evaluation
     * @param func the function to send
     * @param params an object that will be JSON stringified and passed to the function
     */
    callWorker(func, params?: any): Promise<any> {
        return new Promise((resolve, reject) => {
            const workermessageid = this.workermessageid++;
            this.resolvemap[workermessageid] = (result) => {
                if (result && result.error) {
                    reject(result.error);
                } else {
                    resolve(result);
                }
            };
            this.worker.postMessage({
                func: func.toString(),
                params: params,
                id: workermessageid
            });
        });
    }

    callWorker2(func, params?: any): Observable<any> {
        return new Observable((observer) => {
            const workermessageid = this.workermessageid++;
            this.resolvemap[workermessageid] = (result) => {
                if (result && result.error) {
                    observer.error(result.error);
                } else {
                    observer.next(result);
                }
            };
            this.worker.postMessage({
                func: func.toString(),
                params: params,
                id: workermessageid
            });
        });
    }

    clone(url: string): Observable<any> {
        return this.workerReady
            .pipe(
                mergeMap(() =>
                    new Observable(observer => {
                    this.callWorker((params) => {
                            self.jsgitclone(params.url, self.workdir);
                            FS.chdir(self.workdir);
                            self.jsgitsetuser('test', 'test@example.com');
                        }, {url: url})
                        .then(() => observer.next());
                    })
                ),
                mergeMap(() => this.syncLocalFS(false)),
                tap(() => this.repositoryOpen = true),
                tap(() => this.updateAllDirListeners()),
            );
    }

    readdir(): Observable<FileInfo[]> {
        return this.callWorker2(() =>
                FS.readdir('.')
                    .map(name => Object.assign({
                        name: name,
                        fullpath: FS.cwd() + '/' + name
                    }, FS.stat(name))
                    )
                    .map(fileinfo =>
                        Object.assign(fileinfo,
                            { isDir: FS.isDir(fileinfo.mode) }
                        )
                    )
            )
            .pipe(
                mergeMap((files: FileInfo[]) =>
                    this.repositoryOpen ?
                        this.listConflicts().pipe(
                            map(cf => files.map(f =>
                                Object.assign(f, {
                                    conflict: cf.findIndex(c =>
                                            f.fullpath.substring(('/' + this.mountdir + '/').length) === c) > -1
                                }))
                            )
                        ) :
                        of(files)
                ),
                tap((ret: FileInfo[]) =>
                    this.fileList.next(ret)
                ),
                tap(() => this.currentpath.pipe(take(1))
                    .subscribe(p => this.updateDirListener(p.join('/')))
            )
        );
    }

    changedir(name: string): Observable<any> {
        return fromPromise(this.callWorker((params) => FS.chdir(params.name), {name: name})
            ).pipe(
                mergeMap(() => this.readdir()),
                mergeMap(() => fromPromise(this.callWorker(() =>
                    self.toGitPath('').split(/\//).filter(p => p.length > 0)
                ))),
                map((cwd) => this.currentpath.next(cwd))
            );
    }

    unlink(filename: string): Observable<any> {
        return fromPromise(
            this.callWorker((params) => {
                FS.unlink(params.filename);
                console.log('Deleted local file', self.toGitPath(params.filename));
            }, {filename: filename})
        ).pipe(
            mergeMap(() => this.readdir()),
            mergeMap(() => this.syncLocalFS(false))
        );
    }

    listConflicts(): Observable<string[]> {
        return this.callWorker2(() => {
            self.jsgitstatus();
            console.log('listconflicts:', self.jsgitstatusresult);
            return self.jsgitstatusresult.filter(r => r.status === 'conflict')
                .map(c => c.our);
        });
    }

    commitChanges(): Observable<any> {
        return fromPromise(this.callWorker((params) => {
            self.jsgitstatus();

            /**
             * Filters the jsgitstatusresult array to exclude items with 'conflict' status
             * @param {void} No parameters
             * @returns {Array} An array of status results without 'conflict' status
             */
            let noconflictstatusresults = self.jsgitstatusresult.filter(c => c.status !== 'conflict');
            /**
             * Filters the jsgitstatusresult array to get only the items with 'conflict' status
             * @param {Array} self.jsgitstatusresult - An array of git status result objects
             * @returns {Array} An array containing only the git status result objects with 'conflict' status
             */
            const conflictstatusresults = self.jsgitstatusresult.filter(c => c.status === 'conflict');
            if (conflictstatusresults.length > 0) {
                /**
                 * Adds files to the Git staging area for conflict resolution
                 * @param {Array} conflictstatusresults - An array of conflict status objects
                 /**
                  * Filters the jsgitstatusresult array to exclude items with 'conflict' status
                  * @param {void} - This method doesn't take any parameters
                  * @returns {Array} An array of status results without conflict status
                  */
                 * @returns {void} This method does not return a value
                 /**
                  * Processes the results of a no-conflict status check and updates the Git repository accordingly.
                  * @param {Array} noconflictstatusresults - An array of objects containing status information for files/directories.
                  * @returns {void} This function doesn't return a value.
                  */
                 */
                conflictstatusresults.forEach(c => self.jsgitadd(c.our));
                self.jsgitresolvemergecommit();
                self.jsgitstatus();
                noconflictstatusresults = self.jsgitstatusresult.filter(c => c.status !== 'conflict');
            }

            if (self.jsgitworkdirnumberofdeltas() > 0
                || noconflictstatusresults.length > 0) {

                    noconflictstatusresults.forEach(p => {
                    if (p.status === 'deleted') {
                        self.jsgitremove(p.path);
                    } else {
                        if ((p.path as string).endsWith('/')) {
                            FS.readdir(self.fromGitPath(p.path))
                                /**
                                 * Filters an array of strings to exclude items that start with a dot (.)
                                 * @param {string[]} f - The array of strings to filter
                                 * @returns {string[]} A new array containing only the strings that don't start with a dot
                                 */
                                .filter((f: string) => f.charAt(0) !== '.')
                                /**
                                 * Adds each file in the given array to the Git staging area
                                 * @param {Array<string>} f - An array of file names to be added
                                 * @returns {void} This method does not return a value
                                 */
                                .forEach(f =>
                                    self.jsgitadd(p.path + f)
                            );
                        }
                        self.jsgitadd(p.path);
                    }
                });
                self.jsgitaddfileswithchanges();
                self.jsgitcommit(
                    'Revision ' + new Date().toJSON()
                );
                console.log('Changes committed');
            } else {
                console.log('No changes');
            }
        })).pipe(
            /**
             * Synchronizes the local file system with the remote one.
             * @param {boolean} false - Indicates whether to force the synchronization.
             * @returns {Observable<any>} An Observable that completes when the synchronization is finished.
             */
            mergeMap(() => this.syncLocalFS(false))
        /**
         * Calls a worker to create a Blob URL from a file
         * @param {Function} callback - Function to execute in the worker, takes an object with filename and returns a Blob URL
         * @param {Object} params - Object containing the filename
         * @param {string} params.filename - The name of the file to read and convert to a Blob URL
         * @returns {Promise<string>} A promise that resolves to the Blob URL of the file contents
         */
        );
    }

    getDownloadObjectUrl(filename: string): Observable<string> {
        return fromPromise(
            this.callWorker(params => {
                    return URL.createObjectURL(new Blob([FS.readFile(params.filename)], { type: 'application/octet-stream' }));
                }, {filename: filename}
            )
        );
    }

    getTextFileContents(filename: string): Observable<string> {
        /**
         * Reads the contents of a file asynchronously using a worker.
         * @param {string} filename - The name or path of the file to be read.
         * @returns {Promise<string>} A promise that resolves with the contents of the file as a UTF-8 encoded string.
         */
        return fromPromise(this.callWorker(params => {
                return FS.readFile(params.filename, {encoding: 'utf8'});
            }, {filename: filename}
        ));
    }

    saveTextFile(filename: string, content: string): Observable<any> {
        /**
         * Writes content to a file asynchronously using a worker.
         * @param {string} filename - The name of the file to write to.
         * @param {string|Uint8Array} content - The content to be written to the file.
         * @returns {Promise<void>} A promise that resolves when the file has been written successfully.
         */
        return fromPromise(this.callWorker(params => {
                return FS.writeFile(params.filename, params.content);
            }, {filename: filename, content: content}
        ```
        /**
         * Calls a worker to download a file and write it to the file system
         * @param {Function} callback - Function to be executed by the worker
         * @param {Object} params - Object containing URL and file name
         * @param {string} params.url - URL of the file to be downloaded
         * @param {string} params.name - Name of the file to be written
         * @returns {void} This method doesn't return a value
         */
        /**
         * Creates an Observable that downloads a file using XMLHttpRequest and writes it to the file system.
         * @param {File} file - The file object to be downloaded and written.
         * @returns {Observable<string>} An Observable that emits the name of the file once it has been successfully written.
         */
        
        ```        )).pipe(
            ```
            /**
             * Synchronizes the local file system with a remote source
             * @param {boolean} force - Determines whether to force synchronization (default: false)
             * @returns {Observable<any>} An Observable that completes when synchronization is finished
             */
            ```
            mergeMap(() => this.syncLocalFS(false))
        );
    }

    uploadFile(file: File): Observable<any> {
        return new Observable(observer => {
            this.callWorker((params) => {
                const xhr = new XMLHttpRequest();
                xhr.open('GET', params.url, false);
                xhr.responseType = 'arraybuffer';
                xhr.send();
                console.log(xhr.response);
                FS.writeFile(params.name, new Uint8Array(xhr.response), {encoding: 'binary'});

                const gitpath = self.toGitPath(params.name);
                console.log('Written file locally', gitpath);
            }, {url: URL.createObjectURL(file), name: file.name})
                /**
                 * Emits the file name to the observer
                 * @param {File} file - The file object containing the name property
                 * @returns {void} Does not return a value, but emits the file name through the observer
                 /**
                  * Synchronizes the local file system with the remote storage
                  * @param {boolean} false - Indicates whether to force a full sync (false for incremental sync)
                  * @returns {Observable<any>} An Observable that completes when the synchronization is finished
                  */
                 */
                .then(() => {
                    observer.next(file.name);
                });
        }).pipe(
            mergeMap(() => this.syncLocalFS(false))
        );
    }

    mkdir(dirname: string): Observable<any> {
        return fromPromise(
            /**
             * Calls a worker to create a new directory
             * @param {Function} callback - Function to be executed by the worker
             * @param {Object} params - Parameters object containing the directory information
             * @param {string} params.dirname - The name of the directory to be created
             * @returns {void} This method doesn't return a value
             */
            this.callWorker(params => {
                    FS.mkdir(params.dirname);
                }, {dirname: dirname}
            )
        ).pipe(
            /**
             * Merges and maps the result of reading a directory
             * @param {Function} callback - The callback function to be applied to each emitted value
             * @returns {Observable} An Observable that emits the mapped results of reading the directory
             */
            mergeMap(() => this.readdir()),
            ```
            /**
             * Synchronizes the local file system with the remote storage.
             * @param {boolean} false - Indicates whether to force a full sync (true) or perform an incremental sync (false).
             * @returns {Observable<any>} An Observable that emits the result of the synchronization process.
             */
            ```
            mergeMap(() => this.syncLocalFS(false))
        );
    }

    rmdir(dirname: string): Observable<any> {
        /**
         * Recursively deletes a directory and its contents using a worker.
         * @param {string} dirname - The path of the directory to be deleted.
         * @returns {Observable} An Observable that completes when the directory is deleted.
         */
        return this.callWorker2((params) => {
            ```
            /**
             * Recursively deletes all files and directories within a given path, excluding hidden files.
             * @param {string} path - The directory path to start the deletion process from.
             * @returns {void} This function doesn't return a value; it performs deletions as a side effect.
             */
            ```
            const dirnameparts: string[] = params.dirname.split('/');
            if (dirnameparts.length > 1) {
                /**
                 * Filters an array of strings, excluding those that start with a dot
                 * @param {string[]} f - The array of strings to filter
                 * @returns {string[]} A new array containing only the strings that do not start with a dot
                 */
                FS.chdir('/' + dirnameparts.slice(0, dirnameparts.length - 1).join('/'));
                console.log('cwd', FS.cwd());
            }
            const recursedir = (path) =>
                FS.readdir(path)
                    .filter((f: string) => !f.startsWith('.'))
                    /**
                     * Recursively deletes files and directories within a given path
                     * @param {string} path - The path to the directory to be processed
                     * @returns {void} This function doesn't return a value
                     */
                    .forEach((f: string) => {
                        const deletepath = path + '/' + f;
                        console.log('Deleting', deletepath);
                        if (FS.isDir(FS.stat(deletepath).mode)) {
                            recursedir(deletepath);
                            FS.rmdir(deletepath);
                        } else {
                            FS.unlink(deletepath);
                        }
                    });
            /**
             * Merges and maps the result of reading a directory
             * @param {function} () - A function that returns an observable or promise
             * @returns {Observable} An observable that emits the result of reading the directory
             */
            recursedir(params.dirname);

            FS.rmdir(params.dirname);
            console.log('Removed directory', params.dirname);
        }, {dirname: dirname}).pipe(
            mergeMap(() => this.readdir()),
            /**
             * Synchronizes the local file system with the remote storage
             * @param {boolean} force - Indicates whether to force synchronization (default: false)
             * @returns {Observable<void>} An Observable that completes when synchronization is finished
             */
            mergeMap(() => this.syncLocalFS(false))
        );
    }

    rename(oldpath: string, newpath: string): Observable<any> {
        return fromPromise(
            /**
             * Renames a file or directory by calling a worker to perform the operation.
             * @param {string} oldpath - The current path of the file or directory to be renamed.
             * @param {string} newpath - The new path or name for the file or directory.
             * @returns {void} This method doesn't return a value.
             */
            this.callWorker(params => {
                    FS.rename(params.oldpath, params.newpath);
                }, {oldpath: oldpath, newpath: newpath}
            )
        ).pipe(
            /**
             /**
              * Synchronizes the local file system with a remote source
              * @param {boolean} false - Indicates whether to perform a full sync (true) or an incremental sync (false)
              * @returns {Observable<any>} An Observable that emits the result of the synchronization process
              */
             * Merges the current observable with the result of reading a directory
             * @returns {Observable<string[]>} An observable that emits an array of file names in the directory
             */
            mergeMap(() => this.readdir()),
            mergeMap(() => this.syncLocalFS(false))
        );
    /**
     * Calls a worker to execute the jsgitpush function asynchronously
     * @param {Function} callback - A function that calls self.jsgitpush()
     * @param {Object} options - An empty object, possibly for future use
     /**
      * Calls a worker to execute a git pull operation asynchronously.
      * @param {Function} callback - A function that calls self.jsgitpull().
      * @param {Object} options - An empty object, possibly for future use or to maintain consistency with other method signatures.
      * @returns {void} This method doesn't return a value.
      */
     * @returns {void} This method does not return a value
     */
    }

    push() {
        return fromPromise(
            this.callWorker(() => {
                    self.jsgitpush();
                }, {}
            )
        );
    }

    pull() {
        return fromPromise(
            this.callWorker(() => {
                    self.jsgitpull();
                }, {}
            )
        ).pipe(
            /**
             * Performs a mergeMap operation to read the contents of a directory
             * @param {Function} () - A callback function that triggers the readdir operation
             * @returns {Observable} An Observable that emits the directory contents
             */
            mergeMap(() => this.readdir()),
            /**
             * Synchronizes the local file system with the remote state.
             * @param {boolean} force - Indicates whether to force synchronization. Default is false.
             * @returns {Observable<any>} An Observable that emits when the synchronization is complete.
             */
            mergeMap(() => this.syncLocalFS(false)),
            /**
             * Executes a side effect to update all directory listeners
             * @returns {void} This method does not return a value
             */
            tap(() => this.updateAllDirListeners())
        );
    }

    listendir(dir: string): Observable<FileInfo[]> {
        if (!this.dirlisteners[dir]) {
            this.dirlisteners[dir] = new BehaviorSubject([]);
            this.updateDirListener(dir);
        }
        /**
         * Updates all directory listeners
         * @returns {void} This method does not return anything
         */
        return this.dirlisteners[dir];
    }

    updateAllDirListeners() {
        Object.keys(this.dirlisteners).forEach(dir => this.updateDirListener(dir));
    }

    updateDirListener(dir: string) {
        if (!this.dirlisteners[dir]) {
            return;
        }

        /**
         /**
          * Subscribes to the workerReady event and performs file system operations when the worker is ready.
          * @param {string} dir - The directory path to read and monitor.
          * @returns {void} This method doesn't return a value, but updates the dirlisteners object with file information.
          */
         * Asynchronously retrieves and processes files from a specified directory
         * @param {string} dir - The directory path to read files from
         * @returns {void} Does not return a value, but emits the processed file list to subscribers
         */
        setTimeout(() =>
            this.workerReady.subscribe(() => {
                /**
                 ```
                 /**
                  * Reads the contents of a directory and returns an array of file/directory objects with additional information.
                  * @param {Object} params - An object containing the directory path.
                  * @param {string} params.dir - The directory path to read from.
                  * @returns {Array<Object>} An array of objects, each representing a file or directory in the specified path, with properties including name, fullpath, and file stats.
                  */
                 ```                 * Calls a worker function to retrieve and process directory contents.
                 * @param {Function} callback - The callback function to be executed by the worker.
                 * @returns {Array} An array of file information objects for non-directory items in the specified path, or an empty array if the directory doesn't exist.
                 */
                this.callWorker((params) => {
                        const fulldirpath = self.fromGitPath(params.dir);
                        let exists = false;
                        try {
                            FS.stat(fulldirpath);
                            exists = true;
                        } catch (e) {

                        }
                        if (exists) {
                            console.log('update dir listener', fulldirpath, params.dir);
                            return FS.readdir(fulldirpath).map(name => Object.assign({
                                name: name,
                                fullpath: self.fromGitPath(`${params.dir}/${name}`)
                                }, FS.stat(self.fromGitPath(`${params.dir}/${name}`)))
                            )
                            /**
                             * Maps over an array of file information objects and adds an 'isDir' property to each
                             * @param {Array<Object>} fileinfo - An array of objects containing file information
                             * @returns {Array<Object>} An array of file information objects with added 'isDir' property
                             */
                            .map(fileinfo =>
                                Object.assign(fileinfo,
                                    { isDir: FS.isDir(fileinfo.mode) }
                                )
                            /**
                             * Filters out directory entries from a list of file information objects
                             * @param {Array<FileInfo>} fileinfo - An array of file information objects
                             * @returns {Array<FileInfo>} An array of file information objects, excluding directories
                             */
                            ).filter(fileinfo => !fileinfo.isDir);
                        } else {
                            return [];
                        }
                    },
                    {dir: dir}
                /**
                 * Processes the retrieved files and notifies listeners
                 * @param {FileInfo[]} files - An array of FileInfo objects representing the files in the directory
                 * @returns {void} This method doesn't return a value
                 */
                ).then((files: FileInfo[]) => {
                    console.log(dir, files);
                    this.dirlisteners[dir].next(files);
                });
            }),
        0);
    }

    /**
     *
     * @param direction false = write, true = read
     */
    syncLocalFS(direction: boolean): Observable<any> {
        console.log('synclocalfs called', direction, this.syncInProgress);
        if (this.syncInProgress) {
            return of(true);
        } else {
            this.syncInProgress = true;
            /**
             * Synchronizes the file system and returns an Observable
             * @param {boolean} direction - The synchronization direction (true for reload, false for persist)
             * @returns {Observable<void>} An Observable that completes when the synchronization is done
             */
            return new Observable(observer => {
                /**
                 ```
                 ```
                 /**
                  * Synchronizes the file system with the persistent storage.
                  * @param {Object} params - The parameters for the synchronization.
                  * @param {boolean} params.direction - The direction of synchronization. True for loading from persistent storage, false for saving to persistent storage.
                  * @returns {Promise} A promise that resolves when the synchronization is complete.
                  */
                 ```                 /**
                  * Synchronizes the file system asynchronously
                  * @param {Object} params - The parameters for synchronization
                  * @param {boolean} params.direction - The direction of synchronization. True for reloading persisted data, false for persisting data
                  * @returns {Promise<any>} A promise that resolves when synchronization is complete
                  */
                 ```
                 * Calls a worker to synchronize the file system
                 * @param {function} callback - A function that returns a Promise to perform the synchronization
                 * @returns {Promise<any>} A promise that resolves when the synchronization is complete
                 */
                this.callWorker((params) =>
                    new Promise<any>((resolve, reject) =>
                        FS.syncfs(params.direction, () => {
                            console.log(params.direction ? 'reload persisted' : 'persisted');
                            resolve();
                        })
                    ),
                    {direction: direction}
                ```
                /**
                 * Completes the synchronization process and notifies the observer
                 * @param {void} None
                 * @returns {void} Notifies the observer that synchronization is complete
                 */
                ```
                ).then(() => {
                    this.syncInProgress = false;
                    observer.next();
                });
            });
        }
    }

    unmount() {
        if (this.worker) {
            this.worker.terminate();
            this.worker = null;
            console.log('Git backend service terminated');
            this.mountdir = null;
            this.repositoryOpen = false;
        }
    }

    ngOnDestroy() {
        this.unmount();
    }
}
