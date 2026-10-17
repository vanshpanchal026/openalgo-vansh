#!/usr/bin/env node
/**
 * Convert a TradingView Pine Script (//@version=5 or //@version=6) file into OpenScript (.oscript)
 *
 * Usage:
 *   node scripts/convert_pinescript.mjs <input.pine> [output.oscript]
 *   node scripts/convert_pinescript.mjs <input.pine> --install
 */

import { readFileSync, writeFileSync } from 'node:fs';
import { resolve, join, basename, extname } from 'node:path';
import { execFileSync } from 'node:child_process';

const args = process.argv.slice(2);
if (args.length === 0 || args.includes('-h') || args.includes('--help')) {
  console.log('Usage:');
  console.log('  node scripts/convert_pinescript.mjs <input.pine> [output.oscript]');
  console.log('  node scripts/convert_pinescript.mjs <input.pine> --install');
  console.log('  node scripts/convert_pinescript.mjs <input.pine> --install --as custom_name.oscript');
  process.exit(0);
}

const inputFile = resolve(args[0]);
const shouldInstall = args.includes('--install');
const asIdx = args.indexOf('--as');
const customAsName = asIdx !== -1 && args[asIdx + 1] ? args[asIdx + 1] : null;

// Determine output file
let outputFile = null;
if (!shouldInstall) {
  if (args[1] && !args[1].startsWith('--')) {
    outputFile = resolve(args[1]);
  } else {
    const base = basename(inputFile, extname(inputFile));
    outputFile = join(resolve('.'), `${base}.oscript`);
  }
} else {
  const base = customAsName || `${basename(inputFile, extname(inputFile))}.oscript`;
  outputFile = join(resolve('.'), base);
}

// 1. Read Pine Script
let pineSource;
try {
  pineSource = readFileSync(inputFile, 'utf8');
} catch (err) {
  console.error(`Failed to read input file "${inputFile}":`, err.message);
  process.exit(1);
}

// 2. Import module
const REPO_ROOT = resolve('.');
const compilerModulePath = join(REPO_ROOT, 'frontend', 'node_modules', 'openalgo-script', 'dist', 'core', 'index.js');

let importScript;
try {
  const mod = await import(`file://${compilerModulePath.replace(/\\/g, '/')}`);
  importScript = mod.importScript;
} catch (err) {
  console.error(`Failed to load openalgo-script compiler from "${compilerModulePath}":`, err.message);
  process.exit(1);
}

// 3. Convert
console.log(`Converting Pine Script: ${basename(inputFile)} ...`);
const result = importScript(pineSource, { name: basename(inputFile) });

if (result.findings && result.findings.length > 0) {
  console.log('\n--- Converter Findings / Warnings ---');
  for (const f of result.findings) {
    console.log(`[${f.code}] ${f.message}`);
  }
  console.log('-------------------------------------\n');
}

if (!result.source || result.source.trim().length === 0) {
  console.error('Error: Importer produced empty source. Check if the script starts with //@version=5 or //@version=6.');
  process.exit(1);
}

// Write the draft .oscript file
writeFileSync(outputFile, result.source, 'utf8');
console.log(`Successfully generated OpenScript draft: ${outputFile}`);

// 4. If --install is requested, validate and install
if (shouldInstall) {
  console.log(`\nValidating and installing into strategies/openscript/ ...`);
  const validatorScript = join(REPO_ROOT, '.claude', 'skills', 'openscript', 'validate.mjs');
  const validatorArgs = [validatorScript, outputFile, '--install'];
  if (customAsName) {
    validatorArgs.push('--as', customAsName);
  }

  try {
    const out = execFileSync('node', validatorArgs, { encoding: 'utf8' });
    console.log(out);
  } catch (err) {
    console.error('Validation or installation failed:');
    if (err.stdout) console.log(err.stdout);
    if (err.stderr) console.error(err.stderr);
    process.exit(1);
  }
} else {
  console.log(`\nNext step: Validate and install your script into OpenAlgo:`);
  console.log(`  node .claude/skills/openscript/validate.mjs "${outputFile}" --install`);
}
