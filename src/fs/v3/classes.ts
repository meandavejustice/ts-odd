import { CID } from "multiformats"
import { PublicDirectory, PublicFile, PublicNode, PrivateDirectory, PrivateFile, PrivateNode, Namefilter, PrivateForest, PrivateRef } from "wnfs"

import * as Crypto from "../../components/crypto/implementation.js"
import * as Depot from "../../components/depot/implementation.js"
import * as Manners from "../../components/manners/implementation.js"

import { Segments as Path } from "../../path/index.js"

import { UnixTree, Puttable, File, Links, PuttableUnixTree } from "../types.js"
import { BlockStore, DepotBlockStore } from "./store.js"
import { BaseFile } from "../base/file.js"
import { Metadata } from "../metadata.js"
import { loadWasm } from "./wasm.js"


type Dependencies = {
  crypto: Crypto.Implementation
  depot: Depot.Implementation
  manners: Manners.Implementation
}

type Rng = {
  randomBytes(count: number): Uint8Array
}

interface DirEntry {
  name: string
  metadata: {
    version: "3.0.0"
    unixMeta: {
      created: number
      modified: number
      mode: number
      kind: "raw" | "dir" | "file" | "metadata" | "symlink" | "hamtShard"
    }
  }
}

interface OpResult<Directory, A> {
  rootDir: Directory
  result: A
}



// RNG


export function makeRngInterface(crypto: Crypto.Implementation): Rng {
  return {
    /** Returns random bytes of specified length */
    randomBytes(count: number): Uint8Array {
      return crypto.misc.randomNumbers({ amount: count });
    }
  }
}



// ROOT


abstract class Root<
  Directory extends PublicDirectory | PrivateDirectory,
  Node extends PublicNode | PrivateNode
> implements UnixTree, Puttable {

  dependencies: Dependencies
  root: Promise<Directory>
  lastRoot: Directory
  store: BlockStore
  readOnly: boolean

  constructor(
    dependencies: Dependencies,
    root: Directory,
    store: BlockStore,
    readOnly: boolean
  ) {
    this.dependencies = dependencies
    this.root = Promise.resolve(root)
    this.lastRoot = root
    this.store = store
    this.readOnly = readOnly
  }


  // TO BE IMPLEMENTED

  abstract lookupNode(directory: Directory, name: string): Promise<Node>
  abstract put(): Promise<CID>
  abstract rootGetNode(path: Path): Promise<OpResult<Directory, Node | null>>
  abstract rootLs(path: Path): Promise<OpResult<Directory, DirEntry[]>>
  abstract rootMkdir(path: Path): Promise<OpResult<Directory, null>>
  abstract rootMv(from: Path, to: Path): Promise<OpResult<Directory, null>>
  abstract rootRead(path: Path): Promise<OpResult<Directory, Uint8Array>>
  abstract rootRm(path: Path): Promise<OpResult<Directory, null>>
  abstract rootWrite(path: Path, cid: Uint8Array): Promise<OpResult<Directory, null>>


  // 🛠

  private async atomically(fn: (root: Directory) => Promise<Directory>): Promise<void> {
    const root = await this.root
    this.root = fn(root)
    await this.root
  }

  private async withError<T>(operation: Promise<T>, opDescription: string): Promise<T> {
    try {
      return await operation
    } catch (e) {
      console.error(`Error during WASM operation ${opDescription}:`)
      throw e
    }
  }


  // COMMIT

  async putDetailed(): Promise<Depot.PutResult> {
    return {
      cid: await this.put(),
      size: 0, // TODO figure out size
      isFile: false,
    }
  }


  // POSIX INTERFACE

  async add(path: Path, content: Uint8Array): Promise<this> {
    await this.atomically(async root => {
      const { rootDir } = await this.withError(
        this.rootWrite(path, content),
        `write(${path.join("/")})`
      )

      return rootDir
    })

    return this
  }

  async cat(path: Path): Promise<Uint8Array> {
    const { result: cidBytes } = await this.withError(
      this.rootRead(path),
      `read(${path.join("/")})`
    )

    const cid = CID.decode(cidBytes)
    return this.dependencies.depot.getUnixFile(cid)
  }

  async exists(path: Path): Promise<boolean> {
    try {
      await this.rootGetNode(path)
      return true
    } catch {
      return false
    }
  }

  async get(path: Path): Promise<PuttableUnixTree | File | null> {
    const { result: node } = await this.withError(
      this.rootGetNode(path),
      `getNode(${path.join("/")})`
    )

    if (node == null) {
      return null
    }

    if (node.isFile()) {
      const cachedFile = node.asFile()
      const content = await this.cat(path)
      const directory = path.slice(0, -1)
      const filename = path[ path.length - 1 ]

      return new WasmFile(content, directory, filename, this, cachedFile)

    } else if (node.isDir()) {
      const cachedDir = node.asDir()

      return new WasmDirectory(this.readOnly, path, this, cachedDir)
    }

    throw new Error(`Unknown node type. Can only handle files and directories.`)
  }

  async ls(path: Path): Promise<Links> {
    const { result: node } = await this.withError(
      this.rootGetNode(path),
      `ls(${path.join("/")})`
    )

    if (node == null) {
      throw new Error(`Can't ls ${path.join("/")}: No such directory`)
    }

    if (!node.isDir()) {
      throw new Error(`Can't ls ${path.join("/")}: Not a directory`)
    }

    const { result: entries } = await this.withError(
      this.rootLs(path),
      `ls(${path.join("/")})`
    )

    const result: Links = {}

    for (const entry of entries) {
      result[ entry.name ] = {
        name: entry.name,
        isFile: entry.metadata.unixMeta.kind === "file",
        size: 0, // TODO size?
        cid: "DEPRECATED",
      }
    }

    return result
  }

  async mkdir(path: Path): Promise<this> {
    await this.atomically(async root => {
      const { rootDir } = await this.withError(
        this.rootMkdir(path),
        `mkdir(${path.join("/")})`
      )

      return rootDir
    })

    return this
  }

  async mv(from: Path, to: Path): Promise<this> {
    await this.atomically(async root => {
      const { rootDir } = await this.withError(
        this.rootMv(from, to),
        `basicMv(${from.join("/")}, ${to.join("/")})`
      )

      return rootDir
    })

    return this
  }

  async rm(path: Path): Promise<this> {
    await this.atomically(async root => {
      const { rootDir } = await this.withError(
        this.rootRm(path),
        `rm(${path.join("/")})`
      )

      return rootDir
    })

    return this
  }

}


export class PublicRoot extends Root<PublicDirectory, PublicNode> {

  static async empty(dependencies: Dependencies): Promise<PublicRoot> {
    await loadWasm(dependencies)

    const store = new DepotBlockStore(dependencies.depot)
    const root = new PublicDirectory(new Date())

    return new PublicRoot(dependencies, root, store, false)
  }

  static async fromCID(dependencies: Dependencies, cid: CID): Promise<PublicRoot> {
    await loadWasm(dependencies)

    const store = new DepotBlockStore(dependencies.depot)
    const root = await PublicDirectory.load(cid.bytes, store)

    return new PublicRoot(dependencies, root, store, false)
  }


  // IMPLEMENTATION

  lookupNode(directory: PublicDirectory, name: string): Promise<PublicNode> {
    return directory.lookupNode(name, this.store)
  }

  async put(): Promise<CID> {
    const cidBytes = await this.root.then(r => r.store(this.store))
    return CID.decode(cidBytes)
  }

  async rootGetNode(path: Path) { return (await this.root).getNode(path, this.store) }
  async rootLs(path: Path) { return (await this.root).ls(path, this.store) }
  async rootMkdir(path: Path) { return (await this.root).mkdir(path, new Date(), this.store) }
  async rootMv(from: Path, to: Path) { return (await this.root).basicMv(from, to, new Date(), this.store) }
  async rootRead(path: Path) { return (await this.root).read(path, this.store) }
  async rootRm(path: Path) { return (await this.root).rm(path, this.store) }

  async rootWrite(path: Path, content: Uint8Array) {
    const { cid } = await this.dependencies.depot.putChunked(content)
    return (await this.root).write(path, cid.bytes, new Date(), this.store)
  }

}


export class PrivateRoot extends Root<PrivateDirectory, PrivateNode> {

  forest: PrivateForest
  rng: Rng

  constructor(
    dependencies: Dependencies,
    root: PrivateDirectory,
    store: BlockStore,
    readOnly: boolean,
    forest: PrivateForest,
    rng: Rng
  ) {
    super(
      dependencies,
      root,
      store,
      readOnly,
    )

    this.forest = forest
    this.rng = rng
  }


  static async empty(dependencies: Dependencies): Promise<PrivateRoot> {
    await loadWasm(dependencies)

    const store = new DepotBlockStore(dependencies.depot)
    const rng = makeRngInterface(dependencies.crypto)
    const forest = new PrivateForest()
    const root = new PrivateDirectory(new Namefilter(), new Date(), rng)

    return new PrivateRoot(dependencies, root, store, false, forest, rng)
  }

  static async fromCID(dependencies: Dependencies, forestCID: CID, privateRef: PrivateRef): Promise<PrivateRoot> {
    await loadWasm(dependencies)

    const store = new DepotBlockStore(dependencies.depot)
    const rng = makeRngInterface(dependencies.crypto)
    const forest = await PrivateForest.load(forestCID.bytes, store)
    const root = await PrivateNode.load(privateRef, forest, store)

    return new PrivateRoot(dependencies, root, store, false, forest, rng)
  }


  // IMPLEMENTATION

  static searchLatest() { return true }

  lookupNode(directory: PrivateDirectory, name: string): Promise<PrivateNode> {
    return directory.lookupNode(name, PrivateRoot.searchLatest(), this.forest, this.store)
  }

  async put(): Promise<CID> {
    const cidBytes = await this.root.then(r => r.store(this.forest, this.store, this.rng))
    return CID.decode(cidBytes)
  }

  async rootGetNode(path: Path) { return (await this.root).getNode(path, PrivateRoot.searchLatest(), this.forest, this.store) }
  async rootLs(path: Path) { return (await this.root).ls(path, PrivateRoot.searchLatest(), this.forest, this.store) }
  async rootMkdir(path: Path) { return (await this.root).mkdir(path, PrivateRoot.searchLatest(), new Date(), this.forest, this.store, this.rng) }
  async rootMv(from: Path, to: Path) { return (await this.root).basicMv(from, to, PrivateRoot.searchLatest(), new Date(), this.forest, this.store, this.rng) }
  async rootRead(path: Path) { return (await this.root).read(path, PrivateRoot.searchLatest(), this.forest, this.store) }
  async rootRm(path: Path) { return (await this.root).rm(path, PrivateRoot.searchLatest(), this.forest, this.store) }
  async rootWrite(path: Path, content: Uint8Array) { return (await this.root).write(path, PrivateRoot.searchLatest(), content, new Date(), this.forest, this.store, this.rng) }

}



// DIRECTORY


export class WasmDirectory implements UnixTree, Puttable {
  readOnly: boolean

  private directory: string[]
  private root: Root<PublicDirectory | PrivateDirectory, PublicNode | PrivateNode>
  private cachedDir: PublicDirectory | PrivateDirectory

  constructor(readOnly: boolean, directory: string[], root: Root<PublicDirectory | PrivateDirectory, PublicNode | PrivateNode>, cachedDir: PublicDirectory | PrivateDirectory) {
    this.readOnly = readOnly
    this.directory = directory
    this.root = root
    this.cachedDir = cachedDir
  }

  private checkMutability(operation: string) {
    if (this.readOnly) throw new Error(`Directory is read-only. Cannot ${operation}`)
  }

  private async updateCache() {
    const { result: node } = await this.root.rootGetNode(this.directory)
    if (node) this.cachedDir = node.asDir()
  }

  get header(): { metadata: Metadata; previous?: CID } {
    return nodeHeader(this.cachedDir)
  }

  async ls(path: Path): Promise<Links> {
    return await this.root.ls([ ...this.directory, ...path ])
  }

  async mkdir(path: Path): Promise<this> {
    this.checkMutability(`mkdir at ${[ ...this.directory, ...path ].join("/")}`)
    await this.root.mkdir([ ...this.directory, ...path ])
    await this.updateCache()
    return this
  }

  async cat(path: Path): Promise<Uint8Array> {
    return await this.root.cat([ ...this.directory, ...path ])
  }

  async add(path: Path, content: Uint8Array): Promise<this> {
    this.checkMutability(`write at ${[ ...this.directory, ...path ].join("/")}`)
    await this.root.add([ ...this.directory, ...path ], content)
    await this.updateCache()
    return this
  }

  async rm(path: Path): Promise<this> {
    this.checkMutability(`remove at ${[ ...this.directory, ...path ].join("/")}`)
    await this.root.rm([ ...this.directory, ...path ])
    await this.updateCache()
    return this
  }

  async mv(from: Path, to: Path): Promise<this> {
    this.checkMutability(`mv from ${[ ...this.directory, ...from ].join("/")} to ${[ ...this.directory, ...to ].join("/")}`)
    await this.root.mv([ ...this.directory, ...from ], [ ...this.directory, ...to ])
    await this.updateCache()
    return this
  }

  async get(path: Path): Promise<PuttableUnixTree | File | null> {
    return await this.root.get([ ...this.directory, ...path ])
  }

  async exists(path: Path): Promise<boolean> {
    return await this.root.exists([ ...this.directory, ...path ])
  }

  async put(): Promise<CID> {
    return this.root.put()
  }

  async putDetailed(): Promise<Depot.PutResult> {
    return {
      isFile: false,
      size: 0,
      cid: await this.put()
    }
  }

}



// FILE
// This is somewhat of a weird hack of providing a result for a `get()` operation.


export class WasmFile extends BaseFile {
  private directory: string[]
  private filename: string
  private root: Root<PublicDirectory | PrivateDirectory, PublicNode | PrivateNode>
  private cachedFile: PublicFile | PrivateFile

  constructor(content: Uint8Array, directory: string[], filename: string, root: Root<PublicDirectory | PrivateDirectory, PublicNode | PrivateNode>, cachedFile: PublicFile | PrivateFile) {
    super(content)
    this.directory = directory
    this.filename = filename
    this.root = root
    this.cachedFile = cachedFile
  }

  private async updateCache() {
    const { result: node } = await this.root.rootGetNode([ ...this.directory, this.filename ])
    if (node) this.cachedFile = node.asFile()
  }

  get header(): { metadata: Metadata; previous?: CID } {
    return nodeHeader(this.cachedFile)
  }

  async updateContent(content: Uint8Array): Promise<this> {
    await super.updateContent(content)
    await this.updateCache()
    return this
  }

  async putDetailed(): Promise<Depot.PutResult> {
    return {
      isFile: true,
      size: 0,
      cid: await this.put()
    }
  }

}



// NODE HEADER


function nodeHeader(node: PublicFile | PublicDirectory | PrivateFile | PrivateDirectory): { metadata: Metadata; previous?: CID } {
  // There's some differences between the two.
  const meta = node.metadata()
  const metadata: Metadata = {
    isFile: meta.unixMeta.kind === "file",
    version: meta.version,
    unixMeta: {
      _type: meta.unixMeta.kind,
      ctime: Number(meta.unixMeta.created),
      mtime: Number(meta.unixMeta.modified),
      mode: meta.unixMeta.mode,
    }
  }

  const previous = null // TODO: Difficult to get history on private side: node.previousCids()[ 0 ]

  return previous
    ? { metadata, previous: CID.decode(previous) }
    : { metadata }
}