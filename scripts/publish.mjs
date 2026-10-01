import { spawnSync } from 'node:child_process';

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { encoding: 'utf8', ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed (exit ${result.status})`);
  }
  return result.stdout;
}

const clean = spawnSync('git', ['diff-index', '--quiet', 'HEAD', '--'], {
  stdio: 'inherit',
});
if (clean.error) throw clean.error;
if (clean.status === 1) {
  console.error('working tree is dirty; commit the version bump first');
  process.exit(1);
}
if (clean.status !== 0) {
  throw new Error(`git diff-index failed (exit ${clean.status})`);
}

const metadata = JSON.parse(run('cargo', ['metadata', '--no-deps', '--format-version', '1']));
const version = metadata.packages.find((pkg) => pkg.name === 'archive-it-client')?.version;
if (!version) throw new Error('archive-it-client version not found in cargo metadata');

const tag = `v${version}`;
console.log(`tagging ${tag}`);
run('git', ['tag', '-a', tag, '-m', tag], { stdio: 'inherit' });
run('git', ['push', 'origin', tag], { stdio: 'inherit' });
