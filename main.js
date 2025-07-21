const fs = require('fs');
const path = require('path');
const axios = require('axios');
const readline = require('readline');
const Table = require('cli-table3');

const NSE_URL = 'https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv';
const NSE_FILE = 'nse.csv';
const BSE_FILE = 'bse.csv';
const OUTPUT_DIR = './files';
const CHUNK_SIZE = 1000;

async function downloadNSEFile(url, dest) {
  console.log('🔄 Downloading CSV file from NSE...');
  try {
    const response = await axios.get(url, {
      responseType: 'stream',
      timeout: 15000,
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://www.nseindia.com/',
        'Accept': '*/*',
      },
    });

    const writer = fs.createWriteStream(dest);
    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
      writer.on('finish', () => {
        console.log('✅ NSE CSV downloaded successfully.\n');
        resolve();
      });
      writer.on('error', reject);
    });
  } catch (err) {
    throw new Error('❌ NSE download failed or timed out.');
  }
}

function extractSymbols(filePath, prefix) {
  return new Promise((resolve, reject) => {
    const symbols = [];
    const seen = new Set();
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity,
    });

    let isFirstLine = true;
    rl.on('line', (line) => {
      if (isFirstLine) {
        isFirstLine = false;
        return;
      }
      const symbol = line.split(',')[0]?.trim();
      if (symbol && /^[A-Z0-9&.]+$/.test(symbol) && !seen.has(symbol)) {
        symbols.push(`${prefix}:${symbol}`);
        seen.add(symbol);
      }
    });

    rl.on('close', () => {
      console.log(`✅ ${prefix} symbols collected: ${symbols.length}`);
      resolve(symbols);
    });

    rl.on('error', reject);
  });
}

function removeDuplicates(nseList, bseList) {
  const nseSet = new Set(nseList.map((s) => s.replace('NSE:', '')));
  const filtered = bseList.filter((s) => !nseSet.has(s.replace('BSE:', '')));
  console.log(`📊 BSE symbols after removing duplicates: ${filtered.length}`);
  return filtered;
}

function writeToFiles(symbols, chunkSize, dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir);

  const table = new Table({
    head: ['File', 'Symbols'],
    style: { head: ['green'], border: ['grey'] },
    colWidths: [30, 15],
  });

  console.log(`\n📦 Total Merged Symbols: ${symbols.length}\n`);

  let fileNum = 0;
  for (let i = 0; i < symbols.length; i += chunkSize) {
    const chunk = symbols.slice(i, i + chunkSize);
    const fileName = `${++fileNum}.txt`;
    const filePath = path.join(dir, fileName);
    fs.writeFileSync(filePath, chunk.join('\n'), 'utf-8');
    table.push([`📁 ${filePath}`, chunk.length]);
  }

  console.log(table.toString());
  console.log('\n✅ All symbol files saved to ./files/\n');
}

(async () => {
  try {
    await downloadNSEFile(NSE_URL, NSE_FILE);

    const nseSymbols = await extractSymbols(NSE_FILE, 'NSE');
    const bseSymbols = await extractSymbols(BSE_FILE, 'BSE');

    const filteredBSE = removeDuplicates(nseSymbols, bseSymbols);

    // Preserve NSE first, BSE second
    const orderedSymbols = [...nseSymbols, ...filteredBSE];

    writeToFiles(orderedSymbols, CHUNK_SIZE, OUTPUT_DIR);
  } catch (err) {
    console.error('❌ Error:', err.message);
  }
})();
