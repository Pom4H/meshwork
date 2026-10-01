import {
  mkdir,
  readdir,
  readFile,
  writeFile,
  rename,
  rm,
} from "node:fs/promises";
import { resolve, relative, join, sep, extname } from "node:path";
import { createHash } from "node:crypto";

export type SceneBundle = {
  name: string;
  sceneHash: string;
  renderHash: string;
  entry: string;
  runtime: string;
  audioEntry?: string;
  files: Record<string, string>;
};
const sha = (data: string | Uint8Array) =>
  createHash("sha256").update(data).digest("hex");
export const validHash = (value: string) => /^[a-f0-9]{64}$/.test(value);

/** Freeze the built renderer, scene modules and textures into one content address. */
export async function bundleScene(
  enginePath: string,
  entry: string,
  name: string,
  directory = ".meshwork/scenes",
  audioEntry?: string,
) {
  const root = resolve(enginePath),
    entryPath = resolve(root, entry);
  if (!entryPath.startsWith(root + sep) || !/\.(mjs|js)$/.test(entryPath))
    throw Error("Scene entry must be a JS module inside the engine checkout");
  const files: Record<string, Uint8Array> = {};
  const collect = async (folder: string) => {
    for (const item of await readdir(join(root, folder), {
      withFileTypes: true,
    })) {
      const path = join(folder, item.name);
      if (item.isSymbolicLink())
        throw Error("Scene bundles cannot contain symlinks");
      if (item.isDirectory()) await collect(path);
      else if (/\.(js|mjs|json|png|jpg|jpeg|webp|bin)$/.test(item.name))
        files[path.replaceAll(sep, "/")] = await readFile(join(root, path));
    }
  };
  await collect("dist");
  const sceneFolder = relative(root, resolve(entryPath, ".."));
  await collect(sceneFolder);
  const normalizedEntry = relative(root, entryPath).replaceAll(sep, "/");
  if(audioEntry && (!files[audioEntry] || !/\.(mjs|js)$/.test(audioEntry)))
    throw Error("Audio entry must be a JS module in the frozen scene bundle");
  if (!files[normalizedEntry] || !files["dist/runtime/browser.js"])
    throw Error("Build game-engine before bundling the scene");
  const hashes = Object.fromEntries(
    Object.keys(files)
      .sort()
      .map((path) => [path, sha(files[path]!)]),
  );
  const renderHash = sha(
    JSON.stringify(
      Object.entries(hashes).filter(([path]) => path.startsWith("dist/")),
    ),
  );
  const sceneHash = sha(
    JSON.stringify({ entry: normalizedEntry, ...(audioEntry?{audioEntry}:{}), files: hashes }),
  );
  const manifest: SceneBundle = {
    name,
    sceneHash,
    renderHash,
    entry: normalizedEntry,
    runtime: "dist/runtime/browser.js",
    ...(audioEntry?{audioEntry}:{}),
    files: hashes,
  };
  await mkdir(directory, { recursive: true });
  const target = join(directory, sceneHash);
  if (await Bun.file(join(target, "manifest.json")).exists()) return manifest;
  const staging = join(directory, ".staging-" + crypto.randomUUID());
  try {
    for (const [path, bytes] of Object.entries(files)) {
      await mkdir(resolve(staging, path, ".."), { recursive: true });
      await writeFile(join(staging, path), bytes);
    }
    await writeFile(join(staging, "manifest.json"), JSON.stringify(manifest));
    for (let attempt = 0; ; attempt++) {
      try {
        await rename(staging, target);
        break;
      } catch (error) {
        if (
          attempt >= 4 ||
          !["EPERM", "EACCES"].includes(
            (error as NodeJS.ErrnoException).code ?? "",
          )
        )
          throw error;
        // Windows scanners can briefly hold a just-written staging directory.
        await Bun.sleep(25 * (attempt + 1));
      }
    }
  } finally {
    await rm(staging, { recursive: true, force: true });
  }
  return manifest;
}

export async function readScene(
  directory: string,
  hash: string,
): Promise<SceneBundle | undefined> {
  if (!validHash(hash)) return undefined;
  const file = Bun.file(join(directory, hash, "manifest.json"));
  if (!(await file.exists())) return undefined;
  try {
    const scene = (await file.json()) as SceneBundle;
    const hashes = Object.fromEntries(
      Object.entries(scene.files).sort(([a], [b]) =>
        a < b ? -1 : a > b ? 1 : 0,
      ),
    );
    if (
      scene.sceneHash !== hash ||
      scene.runtime !== "dist/runtime/browser.js" ||
      !Object.hasOwn(hashes, scene.entry) ||
      !Object.hasOwn(hashes, scene.runtime) ||
      (scene.audioEntry !== undefined && (typeof scene.audioEntry !== "string" || !/\.(mjs|js)$/.test(scene.audioEntry) || !Object.hasOwn(hashes, scene.audioEntry))) ||
      Object.entries(hashes).some(
        ([path, digest]) =>
          !validHash(digest) ||
          path.includes("\\") ||
          path.startsWith("/") ||
          path
            .split("/")
            .some((p) => p === "." || p === ".." || p.includes(":")),
      ) ||
      sha(JSON.stringify({ entry: scene.entry, ...(scene.audioEntry?{audioEntry:scene.audioEntry}:{}), files: hashes })) !== hash ||
      sha(
        JSON.stringify(
          Object.entries(hashes).filter(([path]) => path.startsWith("dist/")),
        ),
      ) !== scene.renderHash
    )
      return undefined;
    return scene;
  } catch {
    return undefined;
  }
}

export async function serveScene(
  directory: string,
  hash: string,
  path: string,
) {
  const scene = await readScene(directory, hash);
  if (!scene) return new Response("Scene not found", { status: 404 });
  if (!path) return Response.json(scene);
  if (
    !Object.hasOwn(scene.files, path) ||
    path.split("/").some((p) => p === ".." || p === ".")
  )
    return new Response("Asset not found", { status: 404 });
  const file = Bun.file(join(directory, hash, path));
  if (!(await file.exists()))
    return new Response("Asset not found", { status: 404 });
  const bytes = new Uint8Array(await file.arrayBuffer());
  if (sha(bytes) !== scene.files[path])
    return new Response("Scene bundle changed; rebuild it", { status: 409 });
  const mime: Record<string, string> = {
    ".js": "text/javascript",
    ".mjs": "text/javascript",
    ".png": "image/png",
    ".json": "application/json",
  };
  return new Response(bytes, {
    headers: {
      "content-type": mime[extname(path)] ?? file.type,
      "cache-control": "public,max-age=31536000,immutable",
    },
  });
}
