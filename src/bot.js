import 'dotenv/config';
import { Client, GatewayIntentBits, REST, Routes, Partials, ActionRowBuilder, ButtonBuilder, ButtonStyle, ModalBuilder, TextInputBuilder, TextInputStyle, InteractionType, EmbedBuilder } from 'discord.js';
import { upsertWallet, getWallet, ensureSheetSetup, listWalletsWithRow, batchUpdateRoles, batchDeleteRows } from './sheets.js';

const token = process.env.DISCORD_TOKEN;
const clientId = process.env.DISCORD_CLIENT_ID;

if (!token || !clientId) {
	console.error('Missing DISCORD_TOKEN or DISCORD_CLIENT_ID in environment');
	process.exit(1);
}

// Role priority (highest to lowest) - labels match sheet names
const PRIORITY_ROLES = [
	{ id: '1184443552571338792', label: 'Monadian' },
	{ id: '1316432197162631238', label: 'Monarch' },
	{ id: '1184772610102394950', label: 'Monalista' },
];

async function getMemberRoleIds(interaction) {
    if (!interaction.guild) return new Set();
    const member = interaction.member;
    // Try to read roles from the interaction payload
    if (member && member.roles) {
        // Cached GuildMember
        if ('cache' in member.roles) {
            try {
                return new Set(member.roles.cache.map((r) => r.id));
            } catch {}
        }
        // Raw roles array from API payload
        if (Array.isArray(member.roles)) {
            return new Set(member.roles);
        }
    }
    // Fallback: fetch full member (requires GuildMembers intent)
    try {
        const fullMember = await interaction.guild.members.fetch(interaction.user.id);
        return new Set(fullMember.roles.cache.map((r) => r.id));
    } catch {
        return new Set();
    }
}

async function getHighestPriorityRoleLabel(interaction) {
    try {
        const roleIds = await getMemberRoleIds(interaction);
        const found = PRIORITY_ROLES.find((r) => roleIds.has(r.id));
        return found ? found.label : '';
    } catch {
        return '';
    }
}

const client = new Client({
	intents: [
		GatewayIntentBits.Guilds,
		GatewayIntentBits.GuildMembers,
	],
	partials: [Partials.Channel],
});

client.once('ready', () => {
	console.log(`Logged in as ${client.user.tag}`);
});

// No sticky reposting; deletion is governed by Discord permissions (Manage Messages)

// Register commands on startup (guild-scoped if GUILD_ID provided, otherwise global)
async function registerCommands() {
	const commands = [
		{
			name: 'submit-wallet-setup',
			description: 'Post the wallet submission message in this channel',
		},
		{
			name: 'refresh-wallet-roles',
			description: 'Refresh stored roles for submitted wallets',
		},
		{
			name: 'fill-monad-airdrop-role',
			description: 'Fill blank roles with Monad Airdrop if user has that role',
		},
		{
			name: 'prune-no-priority-roles',
			description: 'Remove sheet entries for users without any priority role',
		},
	];
	const rest = new REST({ version: '10' }).setToken(token);
	try {
		if (process.env.GUILD_ID) {
			await rest.put(
				Routes.applicationGuildCommands(clientId, process.env.GUILD_ID),
				{ body: commands },
			);
			console.log('Guild commands registered.');
		} else {
			await rest.put(Routes.applicationCommands(clientId), { body: commands });
			console.log('Global commands registered (may take up to 1 hour to appear).');
		}
	} catch (err) {
		console.error('Failed to register commands:', err);
	}
}

client.on('interactionCreate', async (interaction) => {
	try {
		if (interaction.isChatInputCommand()) {
			if (interaction.commandName === 'submit-wallet-setup') {
				const submitButton = new ButtonBuilder()
					.setCustomId('submit_wallet')
					.setLabel('Submit Wallet')
					.setStyle(ButtonStyle.Success);

				const statusButton = new ButtonBuilder()
					.setCustomId('check_status')
					.setLabel('Check Status')
					.setStyle(ButtonStyle.Primary);

				const row = new ActionRowBuilder().addComponents(submitButton, statusButton);

				const embed = new EmbedBuilder()
					.setDescription('Submit your wallet')
					.setColor(0x2b2d31);

				await interaction.reply({
					embeds: [embed],
					components: [row],
					allowedMentions: { parse: [] },
				});
			}
			if (interaction.commandName === 'refresh-wallet-roles') {
				await interaction.deferReply({ ephemeral: true });
				const items = await listWalletsWithRow();
				await interaction.editReply(`Refreshing roles for ${items.length} user(s). I'll DM you when done.`);
				(async () => {
					const concurrencyLimit = 5;
					const queue = [...items];
					const computed = [];
					const workers = Array.from({ length: concurrencyLimit }, async () => {
						while (queue.length > 0) {
							const item = queue.shift();
							if (!item || !item.discordId) continue;
							try {
								const member = await interaction.guild?.members.fetch(item.discordId).catch(() => null);
								const role = await getHighestPriorityRoleLabel({ guild: interaction.guild, user: { id: item.discordId }, member });
								computed.push({ 
									sheetName: item.sheetName,
									rowNumber: item.rowNumber, 
									discordId: item.discordId,
									discordUsername: item.discordUsername,
									wallet: item.wallet,
									newRole: role || '' 
								});
							} catch {
								computed.push({ 
									sheetName: item.sheetName,
									rowNumber: item.rowNumber, 
									discordId: item.discordId,
									discordUsername: item.discordUsername,
									wallet: item.wallet,
									newRole: '' 
								});
							}
						}
					});
					await Promise.all(workers);
					// Only write differences or moves to reduce API calls
					const diffs = computed
						.map((c) => {
							const existing = items.find((i) => i.discordId === c.discordId);
							// Check if role changed or needs to move to different sheet
							return existing && (existing.role || '') !== c.newRole ? c : null;
						})
						.filter(Boolean);
					let updated = 0;
					let moved = 0;
					if (diffs.length > 0) {
						const result = await batchUpdateRoles(diffs);
						updated = result.updated || 0;
						moved = result.moved || 0;
					}
					try {
						await interaction.user.send(`Roles refreshed: ${updated} updated, ${moved} moved between sheets.`);
					} catch {}
				})();
			}
			if (interaction.commandName === 'fill-monad-airdrop-role') {
				await interaction.deferReply({ ephemeral: true });
				const ROLE_ID = '1427682447369437284';
				const items = await listWalletsWithRow();
				const targets = items.filter((i) => (i.role || '') === '');
				await interaction.editReply(`Checking ${targets.length} user(s) with blank role. I'll DM you when done.`);
				(async () => {
					const concurrencyLimit = 5;
					const queue = [...targets];
					const updates = [];
					const workers = Array.from({ length: concurrencyLimit }, async () => {
						while (queue.length > 0) {
							const item = queue.shift();
							if (!item || !item.discordId) continue;
							try {
								const member = await interaction.guild?.members.fetch(item.discordId).catch(() => null);
								const hasRole = !!member?.roles?.cache?.has(ROLE_ID);
								if (hasRole) {
									updates.push({ 
										sheetName: item.sheetName,
										rowNumber: item.rowNumber,
										discordId: item.discordId,
										discordUsername: item.discordUsername,
										wallet: item.wallet,
										newRole: 'Monad Airdrop'
									});
								}
							} catch {}
						}
					});
					await Promise.all(workers);
					let updated = 0;
					if (updates.length > 0) {
						const result = await batchUpdateRoles(updates);
						updated = result.updated || 0;
					}
					try {
						await interaction.user.send(`Monad Airdrop set for ${updated} user(s).`);
					} catch {}
				})();
			}
			if (interaction.commandName === 'prune-no-priority-roles') {
				await interaction.deferReply({ ephemeral: true });
				const items = await listWalletsWithRow();
				await interaction.editReply(`Scanning ${items.length} user(s) to prune entries without priority roles. I'll DM you when done.`);
				(async () => {
					const concurrencyLimit = 5;
					const queue = [...items];
					const toDelete = [];
					const workers = Array.from({ length: concurrencyLimit }, async () => {
						while (queue.length > 0) {
							const item = queue.shift();
							if (!item || !item.discordId) continue;
							try {
								const member = await interaction.guild?.members.fetch(item.discordId).catch(() => null);
								const roleIds = new Set(member?.roles?.cache?.map((r) => r.id) || []);
								const hasAnyPriority = PRIORITY_ROLES.some((r) => roleIds.has(r.id));
								if (!hasAnyPriority) {
									toDelete.push({ sheetName: item.sheetName, rowNumber: item.rowNumber });
								}
							} catch {}
						}
					});
					await Promise.all(workers);
					let deleted = 0;
					if (toDelete.length > 0) {
						const { deleted: count } = await batchDeleteRows(toDelete);
						deleted = count;
					}
					try {
						await interaction.user.send(`Pruned ${deleted} entrie(s) with no priority roles.`);
					} catch {}
				})();
			}
		}

		if (interaction.isButton()) {
			if (interaction.customId === 'submit_wallet') {
				const modal = new ModalBuilder()
					.setCustomId('wallet_modal')
					.setTitle('Submit your EVM wallet');

				const walletInput = new TextInputBuilder()
					.setCustomId('wallet_address')
					.setLabel('EVM wallet address (0x...)')
					.setStyle(TextInputStyle.Short)
					.setPlaceholder('0x...')
					.setRequired(true)
					.setMaxLength(100);

				const row = new ActionRowBuilder().addComponents(walletInput);
				modal.addComponents(row);
				await interaction.showModal(modal);
			}

			if (interaction.customId === 'check_status') {
				await interaction.deferReply({ ephemeral: true });
				const record = await getWallet(interaction.user.id);
				if (!record) {
					await interaction.editReply('You have not submitted a wallet yet.');
					return;
				}
				const embed = new EmbedBuilder()
					.setTitle('Wallet Submission')
					.addFields(
						{ name: 'Discord Username', value: record.discordUsername || 'Unknown', inline: true },
						{ name: 'Discord ID', value: record.discordId, inline: true },
						{ name: 'EVM Wallet', value: record.wallet || 'N/A' },
						{ name: 'Role', value: record.role || 'N/A', inline: true },
					)
					.setColor(0x2ecc71);
				await interaction.editReply({ embeds: [embed] });
			}
		}

		if (interaction.type === InteractionType.ModalSubmit && interaction.customId === 'wallet_modal') {
			// Defer immediately to avoid interaction timeout on first use
            await interaction.deferReply({ ephemeral: true });

			const wallet = interaction.fields.getTextInputValue('wallet_address').trim();
			// very basic 0x address check; no checksum validation
			const isLikelyEvm = /^0x[a-fA-F0-9]{40}$/.test(wallet);
			if (!isLikelyEvm) {
				await interaction.editReply('Invalid EVM address. Please submit a 0x... address.');
				return;
			}
			const discordId = interaction.user.id;
			const discordUsername = `${interaction.user.username}#${interaction.user.discriminator ?? ''}`.replace(/#$/,'');
			const role = await getHighestPriorityRoleLabel(interaction);
			const result = await upsertWallet({ discordId, discordUsername, wallet, role });
			await interaction.editReply(`Wallet ${result.action === 'updated' ? 'updated' : 'saved'} successfully.`);
		}
	} catch (err) {
		console.error('Interaction error:', err);
		try {
			if ('deferred' in interaction && interaction.deferred) {
				await interaction.editReply('There was an error. Please try again.');
			} else if ('replied' in interaction && interaction.replied) {
				await interaction.followUp({ content: 'There was an error. Please try again.', ephemeral: true });
			} else if (interaction.isRepliable()) {
				await interaction.reply({ content: 'There was an error. Please try again.', ephemeral: true });
			}
		} catch {}
	}
});

// Warm up Sheets (creates sheet and headers if needed) and register commands
await ensureSheetSetup().catch((err) => {
	console.error('Sheets warm-up failed:', err);
});
await registerCommands();
client.login(token);


