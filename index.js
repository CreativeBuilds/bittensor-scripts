// index.js
// Run: node index.js
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { ethers } = require('ethers');
const { JsonRpcProvider } = require("ethers");

// === CONFIGURATION ===
const RPC_URL = process.env.RPC_URL;
if (!RPC_URL) {
  console.error("Please set RPC_URL in your .env file.");
  process.exit(1);
}
const provider = new JsonRpcProvider(RPC_URL);

// Uniswap V3 Nonfungible Position Manager
const NFPM_ADDRESS = '0xC36442b4a4522E871399CD717aBDD847Ab11FE88';
const NFPM_ABI = [
  "function balanceOf(address owner) view returns (uint256)",
  "function tokenOfOwnerByIndex(address owner, uint256 index) view returns (uint256)",
  "function positions(uint256 tokenId) view returns (uint96 nonce, address operator, address token0, address token1, uint24 fee, int24 tickLower, int24 tickUpper, uint128 liquidity, uint256 feeGrowthInside0LastX128, uint256 feeGrowthInside1LastX128, uint128 tokensOwed0, uint128 tokensOwed1)"
];
const positionManager = new ethers.Contract(NFPM_ADDRESS, NFPM_ABI, provider);

// Uniswap V3 Factory (to get pool address)
const FACTORY_ADDRESS = '0x1F98431c8aD98523631AE4a59f267346ea31F984';
const FACTORY_ABI = [
  "function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address)"
];
const factory = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, provider);

// Minimal ABI for Uniswap V3 Pool (we need slot0() and liquidity())
const POOL_ABI = [
  "function slot0() view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)",
  "function liquidity() view returns (uint128)"
];

// === TARGET POOL DETAILS ===
// USDC and your token (0x77E06c9eCCf2E797fd462A92B6D7642EF85b0A44)
// USDC mainnet address is: 0xA0b86991C6218b36c1d19D4a2e9Eb0cE3606EB48
const USDC_ADDRESS = '0xA0b86991C6218b36c1d19D4a2e9Eb0cE3606EB48';
const OTHER_TOKEN = '0x77E06c9eCCf2E797fd462A92B6D7642EF85b0A44';
const FEE = 10000; // 1% fee

// ethers v6 no longer has ethers.constants.AddressZero; define our own:
const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";

// === GLOBALS FOR ROLLING AVERAGES ===
const rollingData = {}; // { address: { values: [percentage, ...] } }
const WINDOW_SIZE = 72; // 72 minutes

// === READ ADDRESSES FROM FILE ===
const addressesFile = path.join(__dirname, 'addresses.txt');
if (!fs.existsSync(addressesFile)) {
  console.error(`File ${addressesFile} not found!`);
  process.exit(1);
}
const addresses = fs.readFileSync(addressesFile, 'utf8')
  .split('\n')
  .map(line => line.trim())
  .filter(line => line !== '');

console.log(`Loaded ${addresses.length} addresses from ${addressesFile}`);

// === GLOBAL STORAGE FOR POSITIONS ===
const positionsByAddress = {}; 
// Format: { "0xAddress": [ { token0, token1, fee, tickLower, tickUpper, liquidity } ] }

// === HELPER: CHECKSUM CONVERSION ===
function toChecksumAddress(address) {
  address = address.toLowerCase();
  if (typeof ethers.getAddress === "function") {
    return ethers.getAddress(address);
  } else if (ethers.utils && typeof ethers.utils.getAddress === "function") {
    return ethers.utils.getAddress(address);
  }
  throw new Error("No available function for checksum conversion");
}

// === HELPER: FETCH POSITIONS FOR ONE ADDRESS ===
async function fetchPositionsForAddress(addr) {
  try {
    const balance = await positionManager.balanceOf(addr);
    const count = Number(balance);
    if (count === 0) return []; // No positions for this address
    const tokenIds = await Promise.all(
      [...Array(count).keys()].map(i => positionManager.tokenOfOwnerByIndex(addr, i))
    );
    const rawPositions = await Promise.all(
      tokenIds.map(id => positionManager.positions(id))
    );
    return rawPositions.map(pos => ({
      token0: pos.token0,
      token1: pos.token1,
      fee: Number(pos.fee), // Convert fee to number for proper filtering
      tickLower: pos.tickLower,
      tickUpper: pos.tickUpper,
      liquidity: Number(pos.liquidity)
    })).filter(pos => {
      const isDirect = (
        pos.token0.toLowerCase() === USDC_ADDRESS.toLowerCase() &&
        pos.token1.toLowerCase() === OTHER_TOKEN.toLowerCase() &&
        pos.fee === FEE
      );
      const isReversed = (
        pos.token0.toLowerCase() === OTHER_TOKEN.toLowerCase() &&
        pos.token1.toLowerCase() === USDC_ADDRESS.toLowerCase() &&
        pos.fee === FEE
      );
      return isDirect || isReversed;
    });
  } catch (err) {
    console.error(`Error fetching positions for ${addr}: ${err}`);
    return [];
  }
}

// === SETUP: FETCH ALL POSITIONS FOR ALL ADDRESSES ===
async function loadAllPositions() {
  console.log("Fetching positions for all addresses...");
  for (const addr of addresses) {
    const pos = await fetchPositionsForAddress(addr);
    positionsByAddress[addr] = pos;
    rollingData[addr] = { values: [] };
    console.log(`Address ${addr} has ${pos.length} target pool positions.`);
  }
}

// === GET TARGET POOL ADDRESS ===
async function getTargetPoolAddress() {
  let totalPositions = 0;
  for (const addr of addresses) {
    totalPositions += (positionsByAddress[addr] || []).length;
  }
  if (totalPositions === 0) {
    console.log("No positions across all addresses; skipping pool lookup.");
    return null;
  }
  const usdc = toChecksumAddress(USDC_ADDRESS);
  const other = toChecksumAddress(OTHER_TOKEN);
  const tokenA = usdc < other ? usdc : other;
  const tokenB = usdc < other ? other : usdc;
  const poolAddress = await factory.getPool(tokenA, tokenB, FEE);
  if (!poolAddress || poolAddress === ZERO_ADDRESS) {
    throw new Error("Target pool not found.");
  }
  console.log(`Target pool address: ${poolAddress}`);
  return poolAddress;
}

// === CHECK IF POSITION IS ACTIVE (IN-RANGE) ===
function isPositionActive(position, currentTick) {
  return currentTick >= position.tickLower && currentTick < position.tickUpper;
}

// === LOG AND SAVE DATA ===
function logAndSave(dataLine) {
  console.log(dataLine);
  fs.appendFileSync('liquidity_log.csv', dataLine + "\n");
}

// === UPDATE LOOP WHEN POOL EXISTS ===
async function updateLoop(poolContract) {
  try {
    const slot0 = await poolContract.slot0();
    const currentTick = slot0.tick;
    const sqrtPriceX96 = slot0.sqrtPriceX96;
    // Calculate current price using the formula: (sqrtPriceX96^2) / 2^192
    const currentPrice = (Number(sqrtPriceX96) ** 2) / Math.pow(2, 192);

    // Get the pool's total active liquidity (from all liquidity providers)
    const poolTotalLiquidity = Number(await poolContract.liquidity());

    let activeLiquidityByAddress = {};
    for (const addr of addresses) {
      const positions = positionsByAddress[addr] || [];
      let activeLiquidity = 0;
      for (const pos of positions) {
        if (isPositionActive(pos, currentTick)) {
          activeLiquidity += pos.liquidity;
        }
      }
      activeLiquidityByAddress[addr] = activeLiquidity;
    }

    const timestamp = new Date().toISOString();
    if (poolTotalLiquidity === 0) {
      // If total pool liquidity is zero, log 0% for everyone.
      for (const addr of addresses) {
        const store = rollingData[addr];
        store.values.push(0);
        if (store.values.length > WINDOW_SIZE) store.values.shift();
        const sum = store.values.reduce((acc, val) => acc + val, 0);
        const rollingAvg = sum / store.values.length;
        // Log: timestamp,address,activeLiquidity,percentageOfDepth,rolling72minAvg,currentTick,currentPrice
        const logLine = `${timestamp},${addr},0,0.00%,${rollingAvg.toFixed(2)}%,${currentTick},${currentPrice.toFixed(6)}`;
        logAndSave(logLine);
      }
      console.log("No active liquidity in pool at the current tick.");
      return;
    }

    // Otherwise, compute each tracked address's percentage share of the pool's total active liquidity.
    for (const addr of addresses) {
      const activeLiquidity = activeLiquidityByAddress[addr];
      const percentage = (activeLiquidity / poolTotalLiquidity) * 100;
      const store = rollingData[addr];
      store.values.push(percentage);
      if (store.values.length > WINDOW_SIZE) store.values.shift();
      const sum = store.values.reduce((acc, val) => acc + val, 0);
      const rollingAvg = sum / store.values.length;
      const logLine = `${timestamp},${addr},${activeLiquidity},${percentage.toFixed(2)}%,${rollingAvg.toFixed(2)}%,${currentTick},${currentPrice.toFixed(6)}`;
      logAndSave(logLine);
    }
  } catch (err) {
    console.error("Error in updateLoop:", err);
  }
}

// === UPDATE LOOP WHEN NO POOL EXISTS ===
async function updateLoopNoPool() {
  // When no pool exists, we log N/A for currentTick and currentPrice.
  const timestamp = new Date().toISOString();
  for (const addr of addresses) {
    const store = rollingData[addr];
    store.values.push(0);
    if (store.values.length > WINDOW_SIZE) store.values.shift();
    const sum = store.values.reduce((acc, val) => acc + val, 0);
    const rollingAvg = sum / store.values.length;
    const logLine = `${timestamp},${addr},0,0.00%,${rollingAvg.toFixed(2)}%,N/A,N/A`;
    logAndSave(logLine);
  }
  console.log("Logged zeros for all addresses (no pool).");
}

// === MAIN FUNCTION ===
async function main() {
  // Write CSV header if it doesn't exist
  if (!fs.existsSync('liquidity_log.csv')) {
    fs.writeFileSync('liquidity_log.csv',
      'timestamp,address,activeLiquidity,percentageOfDepth,rolling72minAvg,currentTick,currentPrice\n'
    );
  }

  await loadAllPositions();
  let poolAddress;
  try {
    poolAddress = await getTargetPoolAddress();
  } catch (err) {
    console.error("Warning:", err.message);
    poolAddress = null;
  }

  if (!poolAddress) {
    console.log("No valid pool address found; running update loop with zeros only.");
    await updateLoopNoPool();
    setInterval(updateLoopNoPool, 60_000);
    return;
  }

  const poolContract = new ethers.Contract(poolAddress, POOL_ABI, provider);
  console.log("Starting update loop (once per minute)...");
  await updateLoop(poolContract);
  setInterval(() => {
    updateLoop(poolContract);
  }, 60_000);
}

main().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});
