import { AppInfo } from "../appInfo.js"
import { Maybe } from "../common/types.js"


export enum RootBranch {
  Public = "public",
  Pretty = "p",
  Private = "private",
  PrivateLog = "privateLog",
  PrivateRef = "privateRef",
  Shared = "shared",
  SharedCounter = "sharedCounter",
  Version = "version"
}

export enum Kind {
  Directory = "directory",
  File = "file"
}

export type Segment = string
export type Segments = Segment[]
export type SegmentsNonEmpty = [ Segment, ...Segments ]
export type Partitioned<P> = [ P, ...Segments ]
export type PartitionedNonEmpty<P> = [ P, Segment, ...Segments ]

/**
 * Private partition
 */
export type Private = "private" | RootBranch.Private

/**
 * Public partition
 */
export type Public = "public" | RootBranch.Public

/**
 * `RootBranch`es that are accessible through the POSIX file system interface.
 */
export type Partition = Private | Public

/**
 * A directory path.
 */
export type DirectoryPath<P> = { directory: P }

/**
 * A file path.
 */
export type FilePath<P> = { file: P }

/**
 * A file or directory path.
 */
export type DistinctivePath<P> = DirectoryPath<P> | FilePath<P>

/**
 * Alias for `DirectoryPath`
 */
export type Directory<P> = DirectoryPath<P>

/**
 * Alias for `FilePath`
 */
export type File<P> = FilePath<P>

/**
 * Alias for `DistinctivePath`
 */
export type Distinctive<P> = DistinctivePath<P>



// CREATION


/**
 * Utility function to create a `DirectoryPath`
 */
export function directory<P extends Partition>(...args: PartitionedNonEmpty<P>): DirectoryPath<PartitionedNonEmpty<P>>
export function directory<P extends Partition>(...args: Partitioned<P>): DirectoryPath<Partitioned<P>>
export function directory(...args: SegmentsNonEmpty): DirectoryPath<SegmentsNonEmpty>
export function directory(...args: Segments): DirectoryPath<Segments>
export function directory(...args: Segments): DirectoryPath<Segments> {
  if (args.some(p => p.includes("/"))) {
    throw new Error("Forward slashes `/` are not allowed")
  }
  return { directory: args }
}

/**
 * Utility function to create a `FilePath`
 */
export function file<P extends Partition>(...args: PartitionedNonEmpty<P>): FilePath<PartitionedNonEmpty<P>>
export function file(...args: SegmentsNonEmpty): FilePath<SegmentsNonEmpty>
export function file(...args: Segments): FilePath<Segments>
export function file(...args: Segments): FilePath<Segments> {
  if (args.some(p => p.includes("/"))) {
    throw new Error("Forward slashes `/` are not allowed")
  }
  return { file: args }
}

/**
 * Utility function to create a root `DirectoryPath`
 */
export function root(): DirectoryPath<Segments> {
  return { directory: [] }
}

/**
 * Utility function create an app data path.
 */
export function appData(app: AppInfo): DirectoryPath<PartitionedNonEmpty<Private>>
export function appData(app: AppInfo, suffix: FilePath<Segments>): FilePath<PartitionedNonEmpty<Private>>
export function appData(app: AppInfo, suffix: DirectoryPath<Segments>): DirectoryPath<PartitionedNonEmpty<Private>>
export function appData(app: AppInfo, suffix: DistinctivePath<Segments>): DistinctivePath<PartitionedNonEmpty<Private>>
export function appData(app: AppInfo, suffix?: DistinctivePath<Segments>): DistinctivePath<PartitionedNonEmpty<Private>> {
  const appDir = directory(RootBranch.Private, "Apps", app.creator, app.name)
  return suffix ? combine(appDir, suffix) : appDir
}



// POSIX


/**
 * Transform a string into a `DistinctivePath`.
 *
 * Directories should have the format `path/to/dir/` and
 * files should have the format `path/to/file`.
 *
 * Leading forward slashes are removed too, so you can pass absolute paths.
 */
export function fromPosix(path: string): DistinctivePath<Segments> {
  const split = path.replace(/^\/+/, "").split("/")
  if (path.endsWith("/")) return { directory: split.slice(0, -1) }
  else if (path === "") return root()
  return { file: split }
}

/**
 * Transform a `DistinctivePath` into a string.
 *
 * Directories will have the format `path/to/dir/` and
 * files will have the format `path/to/file`.
 */
export function toPosix(
  path: DistinctivePath<Segments>,
  { absolute }: { absolute: boolean } = { absolute: false }
): string {
  const prefix = absolute ? "/" : ""
  const joinedPath = unwrap(path).join("/")
  if (isDirectory(path)) return prefix + joinedPath + (joinedPath.length ? "/" : "")
  return prefix + joinedPath
}



// 🛠


/**
 * Combine two `DistinctivePath`s.
 */
export function combine<P extends Partition>(a: DirectoryPath<PartitionedNonEmpty<P>>, b: FilePath<Segments>): FilePath<PartitionedNonEmpty<P>>
export function combine<P extends Partition>(a: DirectoryPath<Partitioned<P>>, b: FilePath<SegmentsNonEmpty>): FilePath<PartitionedNonEmpty<P>>
export function combine<P extends Partition>(a: DirectoryPath<Partitioned<P>>, b: FilePath<Segments>): FilePath<Partitioned<P>>
export function combine(a: DirectoryPath<Segments>, b: FilePath<Segments>): FilePath<Segments>
export function combine<P extends Partition>(a: DirectoryPath<PartitionedNonEmpty<P>>, b: DirectoryPath<Segments>): DirectoryPath<PartitionedNonEmpty<P>>
export function combine<P extends Partition>(a: DirectoryPath<Partitioned<P>>, b: DirectoryPath<SegmentsNonEmpty>): DirectoryPath<PartitionedNonEmpty<P>>
export function combine<P extends Partition>(a: DirectoryPath<Partitioned<P>>, b: DirectoryPath<Segments>): DirectoryPath<Partitioned<P>>
export function combine(a: DirectoryPath<Segments>, b: DirectoryPath<Segments>): DirectoryPath<Segments>
export function combine<P extends Partition>(a: DirectoryPath<PartitionedNonEmpty<P>>, b: DistinctivePath<Segments>): DistinctivePath<PartitionedNonEmpty<P>>
export function combine<P extends Partition>(a: DirectoryPath<Partitioned<P>>, b: DistinctivePath<SegmentsNonEmpty>): DistinctivePath<PartitionedNonEmpty<P>>
export function combine<P extends Partition>(a: DirectoryPath<Partitioned<P>>, b: DistinctivePath<Segments>): DistinctivePath<Partitioned<P>>
export function combine(a: DirectoryPath<Segments>, b: DistinctivePath<Segments>): DistinctivePath<Segments>
export function combine(a: DirectoryPath<Segments>, b: DistinctivePath<Segments>): DistinctivePath<Segments> {
  return map(p => unwrap(a).concat(p), b)
}

/**
 * Is this `DistinctivePath` a directory?
 */
export function isDirectory<P>(path: DistinctivePath<P>): path is DirectoryPath<P> {
  return !!(path as DirectoryPath<P>).directory
}

/**
 * Is this `DistinctivePath` a file?
 */
export function isFile<P>(path: DistinctivePath<P>): path is FilePath<P> {
  return !!(path as FilePath<P>).file
}

/**
 * Is this `DistinctivePath` of the given `Partition`?
 */
export function isPartition(partition: Partition, path: DistinctivePath<Segments>): boolean {
  return unwrap(path)[ 0 ] === partition
}

/**
 * Is this `DistinctivePath` on the given `RootBranch`?
 */
export function isOnRootBranch(rootBranch: RootBranch, path: DistinctivePath<Segments>): boolean {
  return unwrap(path)[ 0 ] === rootBranch
}

/**
 * Is this `DirectoryPath` a root directory?
 */
export function isRootDirectory(path: DirectoryPath<Segments>): boolean {
  return path.directory.length === 0
}

/**
 * Check if two `DistinctivePath` have the same `Partition`.
 */
export function isSamePartition(a: DistinctivePath<Segments>, b: DistinctivePath<Segments>): boolean {
  return unwrap(a)[ 0 ] === unwrap(b)[ 0 ]
}

/**
 * Check if two `DistinctivePath` are of the same kind.
 */
export function isSameKind(a: DistinctivePath<Segments>, b: DistinctivePath<Segments>): boolean {
  if (isDirectory(a) && isDirectory(b)) return true
  else if (isFile(a) && isFile(b)) return true
  else return false
}

/**
 * What `Kind` of path are we dealing with?
 */
export function kind(path: DistinctivePath<Segments>): Kind {
  if (isDirectory(path)) return Kind.Directory
  return Kind.File
}

/**
 * Map a `DistinctivePath`.
 */
export function map<A, B>(fn: (p: A) => B, path: DistinctivePath<A>): DistinctivePath<B> {
  if (isDirectory(path)) return { directory: fn(path.directory) }
  else if (isFile(path)) return { file: fn(path.file) }
  return path
}

/**
 * Get the parent directory of a `DistinctivePath`.
 */
export function parent(path: DistinctivePath<[ Partition, Segment, Segment, ...Segments ]>): DirectoryPath<PartitionedNonEmpty<Partition>>
export function parent(path: DistinctivePath<[ Segment, Segment, Segment, ...Segments ]>): DirectoryPath<SegmentsNonEmpty>
export function parent(path: DistinctivePath<PartitionedNonEmpty<Partition>>): DirectoryPath<Partitioned<Partition>>
export function parent(path: DistinctivePath<[ Partition, Segment ]>): DirectoryPath<Partitioned<Partition>>
export function parent(path: DistinctivePath<Partitioned<Partition>>): DirectoryPath<Segments>
export function parent(path: DistinctivePath<SegmentsNonEmpty>): DirectoryPath<Segments>
export function parent(path: DistinctivePath<[ Segment ]>): DirectoryPath<[]>
export function parent(path: DistinctivePath<[]>): null
export function parent(path: DistinctivePath<Segments>): Maybe<DirectoryPath<Segments>>
export function parent(path: DistinctivePath<Segments>): Maybe<DirectoryPath<Segments>> {
  return isDirectory(path) && isRootDirectory(path as DirectoryPath<Segments>)
    ? null
    : directory(...unwrap(path).slice(0, -1))
}

/**
 * Remove the `Partition` of a `DistinctivePath` (ie. the top-level directory)
 */
export function removePartition(path: DistinctivePath<Segments>): DistinctivePath<Segments> {
  return map(
    p => isDirectory(path) || p.length > 1 ? p.slice(1) : p,
    path
  )
}

/**
 * Get the last part of the path.
 */
export function terminus(path: DistinctivePath<Segments>): Maybe<string> {
  const u = unwrap(path)
  if (u.length < 1) return null
  return u[ u.length - 1 ]
}

/**
 * Unwrap a `DistinctivePath`.
 */
export function unwrap<P>(path: DistinctivePath<P>): P {
  if (isDirectory(path)) {
    return path.directory
  } else if (isFile(path)) {
    return path.file
  }

  throw new Error("Path is neither a directory or a file")
}

/**
 * Utility function to prefix a path with a `Partition`.
 */
export function withPartition<P extends Partition>(partition: P, path: DirectoryPath<SegmentsNonEmpty>): DirectoryPath<PartitionedNonEmpty<P>>
export function withPartition<P extends Partition>(partition: P, path: DirectoryPath<Segments>): DirectoryPath<Partitioned<P>>
export function withPartition<P extends Partition>(partition: P, path: FilePath<SegmentsNonEmpty>): FilePath<PartitionedNonEmpty<P>>
export function withPartition<P extends Partition>(partition: P, path: FilePath<Segments>): FilePath<Partitioned<P>>
export function withPartition<P extends Partition>(partition: P, path: DistinctivePath<SegmentsNonEmpty>): DistinctivePath<PartitionedNonEmpty<P>>
export function withPartition<P extends Partition>(partition: P, path: DistinctivePath<Segments>): DistinctivePath<Partitioned<P>>
export function withPartition<P extends Partition>(partition: P, path: DistinctivePath<Segments>): DistinctivePath<Partitioned<P>> {
  return combine(
    directory(partition),
    path
  )
}



// ⚗️


/**
 * Render a raw `Path` to a string for logging purposes.
 */
export function log(path: Segments): string {
  return `[ ${path.join(", ")} ]`
}
