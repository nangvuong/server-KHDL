const express = require("express");
const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const app = express();
const PORT = process.env.PORT || 3000;

// CORS để frontend gọi API
const cors = require("cors");
app.use(cors());

// File CSV (ưu tiên file processed nếu có)
const CSV_PATHS = [
  path.join(__dirname, "data", "crypto_market_full.csv"),
  path.join(__dirname, "data.csv")
];

let allCoinsFromCSV = null; // cache array of objects
let totalCoinsAvailable = 0;

function findExistingCsvPath() {
  for (const p of CSV_PATHS) {
    if (fs.existsSync(p)) return p;
  }
  return null;
}

function loadCSV(csvPath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(csvPath)
      .pipe(csv())
      .on("data", (row) => results.push(row))
      .on("end", () => resolve(results))
      .on("error", (err) => reject(err));
  });
}

async function ensureLoaded() {
  if (allCoinsFromCSV && Array.isArray(allCoinsFromCSV) && allCoinsFromCSV.length > 0) return;
  const csvPath = findExistingCsvPath();
  if (!csvPath) {
    console.warn("No CSV file found in data/ folder");
    allCoinsFromCSV = [];
    totalCoinsAvailable = 0;
    return;
  }
  try {
    console.log(`📥 Loading CSV: ${csvPath} ...`);
    allCoinsFromCSV = await loadCSV(csvPath);
    totalCoinsAvailable = allCoinsFromCSV.length;
    console.log(`✅ Loaded ${totalCoinsAvailable} rows from CSV`);
  } catch (err) {
    console.error("Error loading CSV:", err);
    allCoinsFromCSV = [];
    totalCoinsAvailable = 0;
  }
}

function normalizeValue(value) {
  // Chuyển sang Number nếu có thể
  if (value === null || value === undefined || value === "") return 0;
  if (!isNaN(value)) return Number(value);
  // Nếu là string kiểu dict JSON, parse
  if (typeof value === "string" && value.startsWith("{") && value.endsWith("}")) {
    try {
      return JSON.parse(value.replace(/'/g, '"')); // replace ' bằng "
    } catch (err) {
      return value; // không parse được thì giữ nguyên string
    }
  }
  return value; // giữ nguyên
}

// Helper: Chuẩn hóa dữ liệu coin với các cột chính
function normalizeCoinData(coin) {
  const parseNum = (val) => {
    if (val === null || val === undefined || val === "") return null;
    const num = parseFloat(val);
    return Number.isFinite(num) ? num : null;
  };

  const parseDate = (val) => {
    if (!val) return null;
    try {
      const date = new Date(val);
      return !isNaN(date.getTime()) ? date.toISOString() : null;
    } catch {
      return null;
    }
  };

  const parseROI = (val) => {
    if (!val) return null;
    if (typeof val === "string") {
      try {
        return JSON.parse(val.replace(/'/g, '"'));
      } catch {
        return null;
      }
    }
    return null;
  };

  return {
    id: String(coin.id || "").trim(),
    symbol: String(coin.symbol || "").trim().toLowerCase(),
    name: String(coin.name || "").trim(),
    image: String(coin.image || "").trim(),
    current_price: parseNum(coin.current_price),
    market_cap: parseNum(coin.market_cap),
    market_cap_rank: parseNum(coin.market_cap_rank),
    fully_diluted_valuation: parseNum(coin.fully_diluted_valuation),
    total_volume: parseNum(coin.total_volume),
    high_24h: parseNum(coin.high_24h),
    low_24h: parseNum(coin.low_24h),
    price_change_24h: parseNum(coin.price_change_24h),
    price_change_percentage_24h: parseNum(coin.price_change_percentage_24h),
    market_cap_change_24h: parseNum(coin.market_cap_change_24h),
    market_cap_change_percentage_24h: parseNum(coin.market_cap_change_percentage_24h),
    circulating_supply: parseNum(coin.circulating_supply),
    total_supply: parseNum(coin.total_supply),
    max_supply: parseNum(coin.max_supply),
    ath: parseNum(coin.ath),
    ath_change_percentage: parseNum(coin.ath_change_percentage),
    ath_date: parseDate(coin.ath_date),
    atl: parseNum(coin.atl),
    atl_change_percentage: parseNum(coin.atl_change_percentage),
    atl_date: parseDate(coin.atl_date),
    roi: parseROI(coin.roi),
    last_updated: parseDate(coin.last_updated || coin.LastUpdate)
  };
}

app.get("/api/coins", async (req, res) => {
  await ensureLoaded();
  
  let { page = 1, limit = 20, search = "" } = req.query;
  page = parseInt(page) || 1;
  limit = Math.min(250, parseInt(limit) || 20);
  search = (search || "").toLowerCase().trim();

  let filteredCoins = allCoinsFromCSV;
  
  if (search) {
    filteredCoins = allCoinsFromCSV.filter(coin =>
      String(coin.name || "").toLowerCase().includes(search) ||
      String(coin.symbol || "").toLowerCase().includes(search) ||
      String(coin.id || "").toLowerCase().includes(search)
    );
  }

  const totalCoins = filteredCoins.length;
  const startIndex = (page - 1) * limit;
  const endIndex = startIndex + limit;
  const paginatedCoins = filteredCoins.slice(startIndex, endIndex);

  // Chuẩn hóa dữ liệu
  const normalizedCoins = paginatedCoins.map(coin => normalizeCoinData(coin));

  res.json({
    success: true,
    pagination: {
      page: page,
      limit: limit,
      total: totalCoins,
      total_pages: Math.ceil(totalCoins / limit),
      has_next: endIndex < totalCoins,
      has_prev: page > 1
    },
    count: normalizedCoins.length,
    data: normalizedCoins
  });
});

// Endpoint: /api/histogram - Trả về dữ liệu histogram của market_cap (với log scale để trực quan hơn)
// Query params: bins (số khoảng, mặc định: 20)
app.get("/api/histogram", async (req, res) => {
  await ensureLoaded();

  const bins = Math.min(100, Math.max(5, parseInt(req.query.bins) || 20));

  // Lấy tất cả giá trị market_cap hợp lệ
  const marketCapValues = allCoinsFromCSV
    .map(coin => normalizeValue(coin.market_cap))
    .filter(val => typeof val === "number" && val > 0)
    .sort((a, b) => a - b);

  if (marketCapValues.length === 0) {
    return res.json({
      success: true,
      message: "No valid market cap data",
      histogram: [],
      statistics: {
        count: 0,
        min: 0,
        max: 0,
        mean: 0,
        median: 0
      }
    });
  }

  // Tính min, max
  const min = marketCapValues[0];
  const max = marketCapValues[marketCapValues.length - 1];
  const total = marketCapValues.length;

  // Sử dụng log scale để phân bố dữ liệu đều hơn
  const logMin = Math.log10(min);
  const logMax = Math.log10(max);
  const logRange = logMax - logMin;
  const logBinSize = logRange / bins;

  // Tạo histogram bins với log scale
  const histogram = Array(bins).fill(0).map((_, i) => {
    const logStart = logMin + i * logBinSize;
    const logEnd = logMin + (i + 1) * logBinSize;
    const start = Math.pow(10, logStart);
    const end = Math.pow(10, logEnd);
    
    return {
      bin: i + 1,
      label: `$${start.toExponential(1)} - $${end.toExponential(1)}`,
      range: `${formatNumber(start)} - ${formatNumber(end)}`,
      start: parseFloat(start.toFixed(2)),
      end: parseFloat(end.toFixed(2)),
      logStart: parseFloat(logStart.toFixed(2)),
      logEnd: parseFloat(logEnd.toFixed(2)),
      count: 0,
      percentage: 0,
      color: getColor(i, bins)
    };
  });

  // Đếm giá trị rơi vào từng bin
  for (const value of marketCapValues) {
    const logValue = Math.log10(value);
    const binIndex = Math.min(
      bins - 1,
      Math.floor((logValue - logMin) / logBinSize)
    );
    if (binIndex >= 0 && binIndex < bins) {
      histogram[binIndex].count++;
    }
  }

  // Tính percentage và tối ưu hóa dữ liệu
  let maxCount = 0;
  for (const bin of histogram) {
    bin.percentage = parseFloat(((bin.count / total) * 100).toFixed(2));
    maxCount = Math.max(maxCount, bin.count);
  }

  // Tính statistics
  const mean = marketCapValues.reduce((a, b) => a + b, 0) / total;
  const median = total % 2 === 0
    ? (marketCapValues[total / 2 - 1] + marketCapValues[total / 2]) / 2
    : marketCapValues[Math.floor(total / 2)];

  res.json({
    success: true,
    statistics: {
      count: total,
      min: parseFloat(min.toFixed(2)),
      max: parseFloat(max.toFixed(2)),
      mean: parseFloat(mean.toFixed(2)),
      median: parseFloat(median.toFixed(2)),
      range: parseFloat((max - min).toFixed(2)),
      maxCount: maxCount
    },
    bins_count: bins,
    scale: "logarithmic",
    histogram: histogram.filter(bin => bin.count > 0)
  });
});

// Helper: Format số lớn thành dạng dễ đọc
function formatNumber(num) {
  if (num >= 1e9) return (num / 1e9).toFixed(1) + 'B';
  if (num >= 1e6) return (num / 1e6).toFixed(1) + 'M';
  if (num >= 1e3) return (num / 1e3).toFixed(1) + 'K';
  return num.toFixed(0);
}

// Helper: Tạo màu gradient cho từng bin
function getColor(index, total) {
  const hue = (index / total) * 240; // Từ xanh (240°) đến đỏ (0°)
  const saturation = 70 + (index / total) * 30; // Từ 70% đến 100%
  const lightness = 45 + (index / total) * 10; // Từ 45% đến 55%
  return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
}

// Helper: Tính percentile
function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = arr.slice().sort((a, b) => a - b);
  const index = (p / 100) * (sorted.length - 1);
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  const weight = index % 1;
  
  if (lower === upper) return sorted[lower];
  return sorted[lower] * (1 - weight) + sorted[upper] * weight;
}

// Endpoint: /api/scatter - Trả về dữ liệu scatter plot cho 2 cột (tối ưu - chỉ 2D histogram)
// Query params: 
//   - x: numeric column for X axis (default: current_price)
//   - y: numeric column for Y axis (default: market_cap)
//   - bins: số bins trên mỗi axis (default: 15)
app.get("/api/scatter", async (req, res) => {
  await ensureLoaded();

  const xColumn = req.query.x || "current_price";
  const yColumn = req.query.y || "market_cap";
  const bins = Math.min(30, Math.max(5, parseInt(req.query.bins) || 15));

  // Lấy tất cả dữ liệu hợp lệ từ 2 cột
  const allData = allCoinsFromCSV
    .map(coin => {
      const xVal = parseFloat(coin[xColumn]);
      const yVal = parseFloat(coin[yColumn]);
      const isValid = Number.isFinite(xVal) && Number.isFinite(yVal) && xVal > 0 && yVal > 0;
      return isValid ? { x: xVal, y: yVal } : null;
    })
    .filter(item => item !== null);

  if (allData.length === 0) {
    return res.json({
      success: false,
      message: `No valid data for columns: x=${xColumn}, y=${yColumn}`
    });
  }

  // Tính min/max
  const xMin = Math.min(...allData.map(d => d.x));
  const xMax = Math.max(...allData.map(d => d.x));
  const yMin = Math.min(...allData.map(d => d.y));
  const yMax = Math.max(...allData.map(d => d.y));
  const xRange = xMax - xMin || 1;
  const yRange = yMax - yMin || 1;

  // Auto log scale nếu range quá lớn
  const xUseLog = xRange > 1000;
  const yUseLog = yRange > 1000;

  // Tạo 2D bin grid và đếm
  const binGrid = Array(bins).fill(null).map(() => Array(bins).fill(0));

  for (const point of allData) {
    let xBin, yBin;

    if (xUseLog) {
      const logX = Math.log10(point.x);
      const logMin = Math.log10(xMin);
      const logMax = Math.log10(xMax);
      xBin = Math.min(bins - 1, Math.floor(((logX - logMin) / (logMax - logMin)) * bins));
    } else {
      xBin = Math.min(bins - 1, Math.floor(((point.x - xMin) / xRange) * bins));
    }

    if (yUseLog) {
      const logY = Math.log10(point.y);
      const logMin = Math.log10(yMin);
      const logMax = Math.log10(yMax);
      yBin = Math.min(bins - 1, Math.floor(((logY - logMin) / (logMax - logMin)) * bins));
    } else {
      yBin = Math.min(bins - 1, Math.floor(((point.y - yMin) / yRange) * bins));
    }

    if (xBin >= 0 && yBin >= 0) {
      binGrid[xBin][yBin]++;
    }
  }

  // Chuyển grid thành array tối ưu
  const binnedPoints = [];
  const maxCount = Math.max(...binGrid.flat());

  for (let i = 0; i < bins; i++) {
    for (let j = 0; j < bins; j++) {
      const count = binGrid[i][j];
      if (count > 0) {
        binnedPoints.push([i, j, count]); // [x_bin, y_bin, count]
      }
    }
  }

  // Tính correlation
  const xValues = allData.map(d => d.x);
  const yValues = allData.map(d => d.y);
  const xMean = xValues.reduce((a, b) => a + b, 0) / xValues.length;
  const yMean = yValues.reduce((a, b) => a + b, 0) / yValues.length;
  const xDev = xValues.map(x => x - xMean);
  const yDev = yValues.map(y => y - yMean);
  const covariance = xDev.reduce((sum, xd, i) => sum + xd * yDev[i], 0) / xValues.length;
  const xStd = Math.sqrt(xDev.reduce((sum, xd) => sum + xd * xd, 0) / xValues.length);
  const yStd = Math.sqrt(yDev.reduce((sum, yd) => sum + yd * yd, 0) / yValues.length);
  const correlation = covariance / (xStd * yStd);

  res.json({
    success: true,
    x: xColumn,
    y: yColumn,
    bins: bins,
    log: { x: xUseLog, y: yUseLog },
    data: binnedPoints,
    total: allData.length,
    bins_count: binnedPoints.length,
    stats: {
      correlation: parseFloat(correlation.toFixed(4))
    }
  });
});

// Endpoint: /api/heatmap - Trả về ma trận tương quan giữa tất cả các cột numeric
// Query params:
//   - columns: danh sách cột (cách nhau bằng dấu phẩy, nếu không có thì lấy tất cả cột numeric)
app.get("/api/heatmap", async (req, res) => {
  await ensureLoaded();

  let columns;

  if (req.query.columns) {
    // Nếu có query, lấy theo danh sách được cung cấp
    columns = req.query.columns.split(",").map(c => c.trim());
  } else {
    // Nếu không có, tự động detect tất cả cột numeric từ CSV headers
    if (allCoinsFromCSV.length === 0) {
      return res.json({
        success: false,
        message: "No data available"
      });
    }

    const firstRow = allCoinsFromCSV[0];
    columns = Object.keys(firstRow);
  }

  // Bỏ các cột không cần thiết (metadata, ngày tháng, dữ liệu thiếu)
  const excludeColumns = [
    "last_updated", "LastUpdate", "image", "name", "symbol", "id",
    "ath_date", "atl_date", "roi", // ngày tháng và non-numeric
    "fully_diluted_valuation" // cột thường thiếu dữ liệu
  ];
  columns = columns.filter(col => !excludeColumns.includes(col));

  // Lọc cột hợp lệ (numeric)
  columns = columns.filter(col => {
    const sample = allCoinsFromCSV.find(coin => coin[col]);
    if (!sample) return false;
    const val = parseFloat(sample[col]);
    return Number.isFinite(val);
  });

  if (columns.length < 2) {
    return res.json({
      success: false,
      message: "Need at least 2 valid numeric columns",
      found_columns: columns.length
    });
  }

  // Lấy dữ liệu và tính correlation matrix
  const data = {};
  for (const col of columns) {
    data[col] = allCoinsFromCSV
      .map(coin => {
        const val = parseFloat(coin[col]);
        return Number.isFinite(val) ? val : null;
      })
      .filter(val => val !== null);
  }

  // Lọc chỉ giữ cột có ít nhất 80% dữ liệu hợp lệ (strict hơn)
  const minDataPoints = Math.floor(allCoinsFromCSV.length * 0.8);
  const validColumns = columns.filter(col => data[col].length >= minDataPoints);

  if (validColumns.length < 2) {
    return res.json({
      success: false,
      message: `Not enough columns with sufficient data. Need at least 2 columns with 80%+ data points`,
      checked_columns: columns.length,
      valid_columns: validColumns.length,
      required_data_points: minDataPoints,
      total_records: allCoinsFromCSV.length
    });
  }

  // Cập nhật columns và data
  const finalColumns = validColumns;
  const finalData = {};
  for (const col of finalColumns) {
    finalData[col] = data[col];
  }

  // Tính correlation coefficient cho mỗi cặp cột
  const n = Math.min(...finalColumns.map(col => finalData[col].length));
  
  if (n < 2) {
    return res.json({
      success: false,
      message: `Not enough valid data points (${n} found, need at least 2)`,
      valid_columns: finalColumns.length
    });
  }

  // Tạo correlation matrix
  const correlationMatrix = [];

  for (let i = 0; i < finalColumns.length; i++) {
    const row = [];
    
    for (let j = 0; j < finalColumns.length; j++) {
      if (i === j) {
        row.push(1.0);
      } else {
        const col1 = finalData[finalColumns[i]].slice(0, n);
        const col2 = finalData[finalColumns[j]].slice(0, n);

        const mean1 = col1.reduce((a, b) => a + b, 0) / n;
        const mean2 = col2.reduce((a, b) => a + b, 0) / n;

        const dev1 = col1.map(x => x - mean1);
        const dev2 = col2.map(x => x - mean2);

        const covariance = dev1.reduce((sum, d, idx) => sum + d * dev2[idx], 0) / n;
        const std1 = Math.sqrt(dev1.reduce((sum, d) => sum + d * d, 0) / n);
        const std2 = Math.sqrt(dev2.reduce((sum, d) => sum + d * d, 0) / n);

        const corr = covariance / (std1 * std2);
        const correlation = Math.max(-1, Math.min(1, corr)); // Clamp to [-1, 1]

        row.push(parseFloat(correlation.toFixed(4)));
      }
    }
    
    correlationMatrix.push(row);
  }

  res.json({
    success: true,
    columns: finalColumns,
    data_points: n,
    data_completeness: parseFloat(((n / allCoinsFromCSV.length) * 100).toFixed(1)) + "%",
    total_records: allCoinsFromCSV.length,
    correlation_matrix: correlationMatrix,
    description: "Correlation matrix with 80%+ data completeness. Range: -1 (negative) to 1 (positive)."
  });
});

// Endpoint: /api/wordmap - Trả về dữ liệu word map theo market_cap
// Query params:
//   - limit: số coin tối đa (default: 50)
//   - min_market_cap: lọc coin có market_cap >= giá trị này (default: 0)
app.get("/api/wordmap", async (req, res) => {
  await ensureLoaded();

  const limit = Math.min(200, Math.max(5, parseInt(req.query.limit) || 50));
  const minMarketCap = parseFloat(req.query.min_market_cap) || 0;

  // Lọc coin có market_cap hợp lệ
  const validCoins = allCoinsFromCSV
    .map(coin => {
      const marketCap = parseFloat(coin.market_cap);
      const name = String(coin.name || "").trim();
      const symbol = String(coin.symbol || "").trim().toUpperCase();
      
      if (!Number.isFinite(marketCap) || marketCap <= minMarketCap || !name || !symbol) {
        return null;
      }
      
      return {
        name: name,
        symbol: symbol,
        market_cap: marketCap,
        current_price: parseFloat(coin.current_price) || 0,
        image: String(coin.image || "").trim()
      };
    })
    .filter(item => item !== null)
    .sort((a, b) => b.market_cap - a.market_cap)
    .slice(0, limit);

  if (validCoins.length === 0) {
    return res.json({
      success: false,
      message: "No valid coins found for wordmap"
    });
  }

  // Tính min/max market_cap để normalize size
  const marketCaps = validCoins.map(c => c.market_cap);
  const minCap = Math.min(...marketCaps);
  const maxCap = Math.max(...marketCaps);
  const capRange = maxCap - minCap || 1;

  // Tạo word map data
  const wordmapData = validCoins.map((coin, index) => {
    // Normalize market_cap thành size (10 - 100)
    const sizeRatio = (coin.market_cap - minCap) / capRange;
    const size = 10 + sizeRatio * 90; // 10 to 100

    // Color gradient: xanh lục (small) -> vàng -> đỏ (large)
    const hue = 120 - (sizeRatio * 120); // 120 (green) -> 0 (red)
    const saturation = 50 + sizeRatio * 50; // 50% -> 100%
    const lightness = 50 - sizeRatio * 20; // 50% -> 30%

    return {
      id: coin.symbol.toLowerCase(),
      text: `${coin.symbol}`,
      value: coin.market_cap,
      size: parseFloat(size.toFixed(2)),
      rank: index + 1,
      name: coin.name,
      price: coin.current_price,
      image: coin.image,
      color: `hsl(${hue}, ${saturation}%, ${lightness}%)`,
      weight: parseFloat((sizeRatio * 100).toFixed(2))
    };
  });

  res.json({
    success: true,
    count: wordmapData.length,
    limit: limit,
    data: wordmapData,
    statistics: {
      total_market_cap: wordmapData.reduce((sum, c) => sum + c.value, 0),
      min_market_cap: parseFloat(minCap.toFixed(2)),
      max_market_cap: parseFloat(maxCap.toFixed(2)),
      avg_market_cap: parseFloat((wordmapData.reduce((sum, c) => sum + c.value, 0) / wordmapData.length).toFixed(2))
    }
  });
});

app.listen(PORT, () => {
    console.log(`✅ Server running on http://localhost:${PORT}`);
    ensureLoaded();
});
