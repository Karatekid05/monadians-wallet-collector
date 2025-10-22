import { google } from 'googleapis';
import dotenv from 'dotenv';

dotenv.config();

const {
	GOOGLE_SHEETS_SPREADSHEET_ID,
	GOOGLE_SERVICE_ACCOUNT_EMAIL,
	GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY,
} = process.env;

if (!GOOGLE_SHEETS_SPREADSHEET_ID) {
	throw new Error('Missing GOOGLE_SHEETS_SPREADSHEET_ID in environment.');
}
if (!GOOGLE_SERVICE_ACCOUNT_EMAIL) {
	throw new Error('Missing GOOGLE_SERVICE_ACCOUNT_EMAIL in environment.');
}
if (!GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY) {
	throw new Error('Missing GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY in environment.');
}

// Support both raw and \n-escaped private keys
const privateKey = GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY.replace(/\\n/g, '\n');

const auth = new google.auth.JWT({
	email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
	key: privateKey,
	scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

const sheetsApi = google.sheets({ version: 'v4', auth });

// Three separate sheets, one per role priority
const SHEET_NAMES = {
	MONADIANS: 'Monadians',
	MONARCH: 'Monarch',
	MONALISTA: 'Monalista',
};
const HEADER_ROW = ['Discord Username', 'Discord ID', 'EVM Wallet', 'Role'];

// Map role labels to sheet names
function getSheetNameForRole(role) {
	const normalized = (role || '').toLowerCase();
	if (normalized === 'monadian') return SHEET_NAMES.MONADIANS;
	if (normalized === 'monarch') return SHEET_NAMES.MONARCH;
	if (normalized === 'monalista') return SHEET_NAMES.MONALISTA;
	return null; // no sheet for users without these roles
}

async function callWithRetry(requestFn, description = 'Sheets API call') {
	const maxAttempts = 5;
	let attempt = 0;
	let delayMs = 1000;
	while (true) {
		try {
			return await requestFn();
		} catch (err) {
			attempt++;
			const status = err?.code || err?.status || err?.response?.status || err?.cause?.code;
			const isRateLimited = status === 429 || err?.cause?.status === 'RESOURCE_EXHAUSTED';
			if (!isRateLimited || attempt >= maxAttempts) {
				throw err;
			}
			await new Promise((r) => setTimeout(r, delayMs + Math.floor(Math.random() * 250)));
			delayMs = Math.min(delayMs * 2, 15000);
		}
	}
}

export async function ensureSheetSetup() {
	// Ensure all three sheets exist with headers
	const spreadsheetId = GOOGLE_SHEETS_SPREADSHEET_ID;
	const spreadsheet = await callWithRetry(() => sheetsApi.spreadsheets.get({ spreadsheetId }), 'spreadsheets.get');
	
	const existingSheets = new Set(
		spreadsheet.data.sheets?.map((s) => s.properties?.title) || []
	);
	
	// Create any missing sheets
	const sheetsToCreate = [];
	for (const sheetName of Object.values(SHEET_NAMES)) {
		if (!existingSheets.has(sheetName)) {
			sheetsToCreate.push({ addSheet: { properties: { title: sheetName } } });
		}
	}
	
	if (sheetsToCreate.length > 0) {
		await callWithRetry(() => sheetsApi.spreadsheets.batchUpdate({
			spreadsheetId,
			requestBody: { requests: sheetsToCreate },
		}), 'spreadsheets.batchUpdate create sheets');
	}
	
	// Write header row for each sheet if needed
	for (const sheetName of Object.values(SHEET_NAMES)) {
		const range = `${sheetName}!A1:D1`;
		const current = await callWithRetry(() => sheetsApi.spreadsheets.values.get({ spreadsheetId, range }), 'values.get header');
		const firstRow = current.data.values?.[0] ?? [];
		if (firstRow.length === 0 || HEADER_ROW.some((h, i) => firstRow[i] !== h)) {
			await callWithRetry(() => sheetsApi.spreadsheets.values.update({
				spreadsheetId,
				range,
				valueInputOption: 'RAW',
				requestBody: { values: [HEADER_ROW] },
			}), 'values.update header');
		}
	}
}

export async function upsertWallet({ discordId, discordUsername, wallet, role }) {
	await ensureSheetSetup();
	const spreadsheetId = GOOGLE_SHEETS_SPREADSHEET_ID;
	
	const targetSheet = getSheetNameForRole(role);
	if (!targetSheet) {
		// User doesn't have any priority role, don't save
		return { action: 'skipped', reason: 'no_priority_role' };
	}
	
	// Check all sheets to see if user exists elsewhere
	let existingLocation = null;
	for (const sheetName of Object.values(SHEET_NAMES)) {
		const range = `${sheetName}!A2:D`;
		const resp = await callWithRetry(() => sheetsApi.spreadsheets.values.get({ spreadsheetId, range }), 'values.get check');
		const rows = resp.data.values || [];
		
		for (let i = 0; i < rows.length; i++) {
			if (rows[i][1] === discordId) {
				existingLocation = { sheetName, rowNumber: i + 2 };
				break;
			}
		}
		if (existingLocation) break;
	}
	
	// If user exists in a different sheet, delete from old sheet
	if (existingLocation && existingLocation.sheetName !== targetSheet) {
		await deleteRowFromSheet(existingLocation.sheetName, existingLocation.rowNumber);
		existingLocation = null; // treat as new insert
	}
	
	const targetRange = `${targetSheet}!A2:D`;
	
	if (!existingLocation) {
		// Insert new row in target sheet
		await callWithRetry(() => sheetsApi.spreadsheets.values.append({
			spreadsheetId,
			range: targetRange,
			valueInputOption: 'RAW',
			insertDataOption: 'INSERT_ROWS',
			requestBody: {
				values: [[discordUsername, discordId, wallet, role ?? '']],
			},
		}), 'values.append upsert');
		return { action: 'inserted' };
	}
	
	// Update existing row in same sheet
	const updateRange = `${targetSheet}!A${existingLocation.rowNumber}:D${existingLocation.rowNumber}`;
	await callWithRetry(() => sheetsApi.spreadsheets.values.update({
		spreadsheetId,
		range: updateRange,
		valueInputOption: 'RAW',
		requestBody: {
			values: [[discordUsername, discordId, wallet, role ?? '']],
		},
	}), 'values.update upsert');
	return { action: 'updated' };
}

// Helper function to delete a row from a specific sheet
async function deleteRowFromSheet(sheetName, rowNumber) {
	const spreadsheetId = GOOGLE_SHEETS_SPREADSHEET_ID;
	const spreadsheet = await callWithRetry(() => sheetsApi.spreadsheets.get({ spreadsheetId }), 'spreadsheets.get');
	const sheet = spreadsheet.data.sheets?.find((s) => s.properties?.title === sheetName);
	if (!sheet) return;
	
	const sheetId = sheet.properties.sheetId;
	await callWithRetry(() => sheetsApi.spreadsheets.batchUpdate({
		spreadsheetId,
		requestBody: {
			requests: [{
				deleteDimension: {
					range: {
						sheetId: sheetId,
						dimension: 'ROWS',
						startIndex: rowNumber - 1,
						endIndex: rowNumber,
					},
				},
			}],
		},
	}), 'spreadsheets.batchUpdate delete row');
}

export async function getWallet(discordId) {
	await ensureSheetSetup();
	const spreadsheetId = GOOGLE_SHEETS_SPREADSHEET_ID;
	
	// Search across all three sheets
	for (const sheetName of Object.values(SHEET_NAMES)) {
		const range = `${sheetName}!A2:D`;
		const resp = await callWithRetry(() => sheetsApi.spreadsheets.values.get({ spreadsheetId, range }), 'values.get getWallet');
		const rows = resp.data.values || [];
		for (const row of rows) {
			if (row[1] === discordId) {
				return { discordUsername: row[0], discordId: row[1], wallet: row[2], role: row[3] ?? '' };
			}
		}
	}
	return null;
}

export async function listWallets() {
	await ensureSheetSetup();
	const spreadsheetId = GOOGLE_SHEETS_SPREADSHEET_ID;
	const items = [];
	
	// Collect from all three sheets
	for (const sheetName of Object.values(SHEET_NAMES)) {
		const range = `${sheetName}!A2:D`;
		const resp = await callWithRetry(() => sheetsApi.spreadsheets.values.get({ spreadsheetId, range }), 'values.get list');
		const rows = resp.data.values || [];
		for (const row of rows) {
			if (!row || row.length === 0) continue;
			items.push({
				discordUsername: row[0] ?? '',
				discordId: row[1] ?? '',
				wallet: row[2] ?? '',
				role: row[3] ?? '',
			});
		}
	}
	return items;
}

export async function listWalletsWithRow() {
	await ensureSheetSetup();
	const spreadsheetId = GOOGLE_SHEETS_SPREADSHEET_ID;
	const items = [];
	
	// Collect from all three sheets with sheet info
	for (const sheetName of Object.values(SHEET_NAMES)) {
		const range = `${sheetName}!A2:D`;
		const resp = await callWithRetry(() => sheetsApi.spreadsheets.values.get({ spreadsheetId, range }), 'values.get listWithRow');
		const rows = resp.data.values || [];
		for (let i = 0; i < rows.length; i++) {
			const row = rows[i] || [];
			if (row.length === 0) continue;
			items.push({
				sheetName: sheetName,
				rowNumber: i + 2, // actual sheet row number
				discordUsername: row[0] ?? '',
				discordId: row[1] ?? '',
				wallet: row[2] ?? '',
				role: row[3] ?? '',
			});
		}
	}
	return items;
}

// This function is now handled by upsertWallet which manages cross-sheet moves
export async function updateRole(discordId, role) {
	// Find existing record
	const existing = await getWallet(discordId);
	if (!existing) return false;
	
	// Use upsertWallet to handle potential sheet migration
	await upsertWallet({
		discordId,
		discordUsername: existing.discordUsername,
		wallet: existing.wallet,
		role,
	});
	return true;
}

// New batch update that handles cross-sheet migrations
export async function batchUpdateRoles(updates) {
	if (!Array.isArray(updates) || updates.length === 0) return { updated: 0, moved: 0 };
	await ensureSheetSetup();
	
	let updated = 0;
	let moved = 0;
	
	// Process each update individually to handle sheet migrations
	for (const update of updates) {
		const { sheetName, rowNumber, discordId, discordUsername, wallet, newRole } = update;
		const targetSheet = getSheetNameForRole(newRole);
		
		if (!targetSheet) {
			// User no longer has priority role, delete from current sheet
			await deleteRowFromSheet(sheetName, rowNumber);
			updated++;
			continue;
		}
		
		if (sheetName !== targetSheet) {
			// Need to move to different sheet
			await deleteRowFromSheet(sheetName, rowNumber);
			await upsertWallet({ discordId, discordUsername, wallet, role: newRole });
			moved++;
		} else {
			// Just update role in same sheet
			const spreadsheetId = GOOGLE_SHEETS_SPREADSHEET_ID;
			const updateRange = `${sheetName}!D${rowNumber}:D${rowNumber}`;
			await callWithRetry(() => sheetsApi.spreadsheets.values.update({
				spreadsheetId,
				range: updateRange,
				valueInputOption: 'RAW',
				requestBody: {
					values: [[newRole ?? '']],
				},
			}), 'values.update role');
			updated++;
		}
	}
	
	return { updated, moved };
}

// Updated to work with items that have sheetName and rowNumber
export async function batchDeleteRows(items) {
	await ensureSheetSetup();
	if (!Array.isArray(items) || items.length === 0) return { deleted: 0 };
	
	let deleted = 0;
	
	// Group by sheet for efficient deletion
	const bySheet = {};
	for (const item of items) {
		const { sheetName, rowNumber } = item;
		if (!sheetName || !Number.isInteger(rowNumber) || rowNumber < 2) continue;
		if (!bySheet[sheetName]) bySheet[sheetName] = [];
		bySheet[sheetName].push(rowNumber);
	}
	
	// Delete from each sheet (in reverse order to avoid shifting row numbers)
	for (const [sheetName, rowNumbers] of Object.entries(bySheet)) {
		const sorted = Array.from(new Set(rowNumbers)).sort((a, b) => b - a);
		for (const rowNumber of sorted) {
			await deleteRowFromSheet(sheetName, rowNumber);
			deleted++;
		}
	}
	
	return { deleted };
}



