import * as Manners from "../../components/manners/implementation.js"

import { WASM_WNFS_VERSION } from "../../common/version.js"
import { default as init } from "wnfs"


export async function loadWasm({ manners }: { manners: Manners.Implementation }) {
  manners.log(`⏬ Loading WNFS WASM`)
  const before = performance.now()
  // init accepts Promises as arguments
  await init(manners.wnfsWasmLookup(WASM_WNFS_VERSION))
  const time = performance.now() - before
  manners.log(`🧪 Loaded WNFS WASM (${time.toFixed(0)}ms)`)
}