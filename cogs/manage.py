import discord
from discord.ext import commands, tasks
from discord import app_commands
from typing import Optional
import logging
import asyncio
import difflib
from lib.utils import updateConfig, checkVerification, validateValue, getDatabaseData, listConfig, storeVerifiedUser, getAllDatabaseData
from lib.makeRequest import makeRequest
from datetime import timezone, datetime
#NOTE: This is one of the worst pieces of code ive ever written. This fucking sucks, top to bottom.
#! Commands:
#!  /manage check_for_promotions - Checks against some values if the user is good to be promoted (xp contributed, X num of playtime hours in guild, wars, maybe more idk) 
#!  /manage check_for_demotions - Checks against some values if the user is good to be demoted (prolly like just inactivity?)
#!  /manage update - Manually check and update all user roles

logger = logging.getLogger('discord')
configTypes = {
    #"xp_for_recruiter": "number",
    #"xp_for_captain": "number",
    #"xp_for_strategist": "number",
    #"xp_for_chief": "number",
    #"days_for_recruiter": "number",
    #"days_for_captain": "number",
    #"days_for_strategist": "number",
    #"days_for_chief": "number",
    #"wars_for_recruiter": "number",
    #"wars_for_captain": "number",
    #"wars_for_strategist": "number",
    #"wars_for_chief": "number",
    #"inactivity_days_recruit": "number",
    #"inactivity_days_recruiter": "number",
    #"inactivity_days_captain": "number",
    #"inactivity_days_strategist": "number",
    #"inactivity_days_chief": "number",
    "verification_role": "role",
    "guild_member_role": "role",
    "recruit_role": "role",
    "recruiter_role": "role",
    "captain_role": "role",
    "strategist_role": "role",
    "chief_role": "role",
    "owner_role": "role",
    "level_25plus_role": "role",
    "level_50plus_role": "role",
    "level_75plus_role": "role",
    "level_100plus_role": "role",
    "level_105plus_role": "role",
    "verify_apps": "bool",
    "log": "bool",
    "verify_rank": "guildRank",
    "log_channel": "channel",
    "guild_prefix": "prefix",
}

rankMap = {
    "OWNER": "owner_role",
    "CHIEF": "chief_role",
    "STRATEGIST": "strategist_role",
    "CAPTAIN": "captain_role",
    "RECRUITER": "recruiter_role",
    "RECRUIT": "recruit_role",
}

rankPriority = {
    "OWNER": 6,
    "CHIEF": 5,
    "STRATEGIST": 4,
    "CAPTAIN": 3,
    "RECRUITER": 2,
    "RECRUIT": 1,
}
    
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=True)  
class Manage(commands.GroupCog, name="manage"):
    def __init__(self, bot):
        self.bot = bot
        asyncio.create_task(self.delayStart())
    
    async def delayStart(self): # So we start loop when we have access to data
        await self.bot.wait_until_ready()
        await asyncio.sleep(10)
        self.updateVerifiedUsers.start()

    @tasks.loop(hours=2)
    async def updateVerifiedUsers(self):
        try:
            allVerified = await asyncio.to_thread(getAllDatabaseData)
            for serverID, discordUserID, gameUser in allVerified:
                await asyncio.sleep(5) # a bit of a ratelimit, we have 2 hours until next run we can take our time
                #logger.info(f"Running updateVerifiedUsers for {discordUserID} ({gameUser}) in {serverID}.")
                guild = self.bot.get_guild(serverID)
                if not guild:
                    logger.warning(f"Bot not in guild {serverID}. Sad sight to see.")
                    continue
                try:
                    user_id = int(discordUserID)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid user ID '{discordUserID}' in database for guild {serverID}.")
                    continue

                await self.forceVerifyForLoop(guild, user_id, gameUser)

        except Exception as e:
            logger.error(f"Unhandled exception in updateVerifiedUsers: {e}", exc_info=True)
            
    async def verifyUser(self, interaction: discord.Interaction, userGuildPrefix, userGuildRank, jsonData, username):
        dbData = await asyncio.to_thread(getDatabaseData, interaction.guild_id)
        dbGuildPrefix = dbData.get("guild_prefix")
        if dbGuildPrefix == userGuildPrefix: # User in guild, give them guild-specific roles
            # Add guild member role
            dbGuildRoleId = dbData.get("guild_member_role")
            if dbGuildRoleId:
                role = interaction.guild.get_role(int(dbGuildRoleId))
                await interaction.user.add_roles(role)
            # Add rank role
            dbGuildRoleId = dbData.get(rankMap[userGuildRank])
            if dbGuildRoleId:
                role = interaction.guild.get_role(int(dbGuildRoleId))
                await interaction.user.add_roles(role)
        # Add verification role
        dbGuildRoleId = dbData.get("verification_role")
        if dbGuildRoleId:
            role = interaction.guild.get_role(int(dbGuildRoleId))
            await interaction.user.add_roles(role)
        # Add Level roles
        highestLevel = 1
        for charID, charData in jsonData["characters"].items():
            if charData["level"] > highestLevel:
                highestLevel = charData["level"]
        db25LevelRole = dbData.get("level_25plus_role")
        db50LevelRole = dbData.get("level_50plus_role")
        db75LevelRole = dbData.get("level_75plus_role")
        db100LevelRole = dbData.get("level_100plus_role")
        db105LevelRole = dbData.get("level_105plus_role")
        if highestLevel >= 25 and db25LevelRole:
            role = interaction.guild.get_role(int(db25LevelRole))
            await interaction.user.add_roles(role)
        if highestLevel >= 50 and db50LevelRole:
            role = interaction.guild.get_role(int(db50LevelRole))
            await interaction.user.add_roles(role)
        if highestLevel >= 75 and db75LevelRole:
            role = interaction.guild.get_role(int(db75LevelRole))
            await interaction.user.add_roles(role)
        if highestLevel >= 100 and db100LevelRole:
            role = interaction.guild.get_role(int(db100LevelRole))
            await interaction.user.add_roles(role)
        if highestLevel >= 105 and db105LevelRole:
            role = interaction.guild.get_role(int(db105LevelRole))
            await interaction.user.add_roles(role)
        await asyncio.to_thread(storeVerifiedUser, interaction.guild_id, str(interaction.user.id), username)

    async def forceVerifyForLoop(self, guild: discord.Guild, discord_user_id, username):
        try:
            member = await guild.fetch_member(discord_user_id)
        except discord.NotFound:
            logger.warning(f"Member ID {discord_user_id} not found in guild {guild.id}") # We should be removing stale entries, however i dont want to implement that rn
            return
        if not member:
            logger.warning(f"Member ID {discord_user_id} not found in guild {guild.id}")
            return
        
        success, r = await asyncio.to_thread(makeRequest, f"https://api.wynncraft.com/v3/player/{username}?fullResult")
        if not success:
            logger.warning(f"Wynncraft user {username} not found for guild {guild.id}")
            return

        jsonData = r.json()
        userGuildPrefix = jsonData.get("guild", {}).get("prefix")
        userGuildRank = jsonData.get("guild", {}).get("rank")
        
        await self.verifyUserForLoop(guild, member, userGuildPrefix, userGuildRank, jsonData, username)

    async def verifyUserForLoop(self, guild, member, userGuildPrefix, userGuildRank, jsonData, username):
        dbData = await asyncio.to_thread(getDatabaseData, guild.id)
        dbGuildPrefix = dbData.get("guild_prefix")
        if dbGuildPrefix == userGuildPrefix:
            dbGuildRoleId = dbData.get("guild_member_role")
            if dbGuildRoleId:
                role = guild.get_role(int(dbGuildRoleId))
                if role:
                    await member.add_roles(role)
            dbGuildRoleId = dbData.get(rankMap.get(userGuildRank))
            if dbGuildRoleId:
                role = guild.get_role(int(dbGuildRoleId))
                if role:
                    await member.add_roles(role)
        dbGuildRoleId = dbData.get("verification_role")
        if dbGuildRoleId:
            role = guild.get_role(int(dbGuildRoleId))
            if role:
                await member.add_roles(role)
        highestLevel = max((charData["level"] for charData in jsonData["characters"].values()), default=1)

        # Add level roles
        level_roles = [
            ("level_25plus_role", 25),
            ("level_50plus_role", 50),
            ("level_75plus_role", 75),
            ("level_100plus_role", 100),
            ("level_105plus_role", 105),
        ]
        for role_name, lvl in level_roles:
            role_id = dbData.get(role_name)
            if highestLevel >= lvl and role_id:
                role = guild.get_role(int(role_id))
                if role:
                    await member.add_roles(role)
        await asyncio.to_thread(storeVerifiedUser, guild.id, member.id, username)

    @app_commands.command(description="Change the config values.")
    @app_commands.describe(
        config_name='The config name to change',
        config_value='The value to update the config_name to',
    )
    async def configure(self, interaction: discord.Interaction, config_name: str, config_value: str):
        requiredRoleName = "Manage Permission"
        #logger.info(f"interaction.user.roles: {interaction.user.roles}")
        permission = 0
        for role in interaction.user.roles:
            #logger.info(f"role.name: {role.name}")
            if role.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return
        
        logger.info(f"Command /manage configure was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter config_name is: {config_name}, config_value is {config_value}.")
        if config_name not in configTypes: # checks if configname is right
            closeMatch = difflib.get_close_matches(config_name, configTypes.keys(), n=1, cutoff=0.4)
            if closeMatch:
                await interaction.response.send_message(f"{config_name} is not valid. Did you mean {closeMatch[0]}?", ephemeral=True)
            else:
                await interaction.response.send_message(f"{config_name} is not valid.", ephemeral=True)
            return

        expected_type = configTypes[config_name]
        try: # checks if config value is right
            validatedValue = await asyncio.to_thread(validateValue, expected_type, config_value, interaction.guild)
        except ValueError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return

        await asyncio.to_thread(updateConfig, interaction.guild_id, config_name, validatedValue)
        await interaction.response.send_message(f"Updated {config_name} to {config_value}.", ephemeral=True)

    @app_commands.command(description="Verify your wynncraft info for this server.")
    @app_commands.describe(username='Your minecraft username.',)
    async def verify(self, interaction: discord.Interaction, username: str):
        logger.info(f"Command /manage verify was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {username}.")
        success, r = await asyncio.to_thread(makeRequest, f"https://api.wynncraft.com/v3/player/{username}?fullResult")
        if not success: # Check if theyre a real user
            await interaction.response.send_message(f"{username} is not a user on wynncraft. Please recheck the username and try again.", ephemeral=True)
            return
        else: # theyre real
            # Check if verify_apps, verify_rank, AND log_channel is set for this discord server
            jsonData = r.json()
            userGuildPrefix = jsonData.get("guild", {}).get("prefix")
            if userGuildPrefix:
                userGuildRank = jsonData["guild"]["rank"]
            verifyApps, verifyRank, logChannel = await asyncio.to_thread(checkVerification, interaction.guild_id)
            if verifyApps in ("true", "yes", "1") and verifyRank and logChannel:
                if rankPriority[userGuildRank] >= rankPriority[verifyRank.upper()]: # they need to be verified
                    embed = discord.Embed(
                        title="Verification Request",
                        description=f"This user has been flagged and needs to be manually verified per your server settings. To accept this user, have an authorized user run `/manage force_verify user_id:{interaction.user.id} username:{username}`. Otherwise, ignore.",
                        color=discord.Color.blue()
                    )
                    embed.add_field(name="Discord Username", value=f"{interaction.user.mention} (`{interaction.user.id}`)", inline=False)
                    embed.add_field(name="Account Created", value=discord.utils.format_dt(interaction.user.created_at, style="F"), inline=True)
                    embed.add_field(name="Joined Server", value=discord.utils.format_dt(interaction.user.joined_at, style="F"), inline=True)
                    embed.add_field(name="Minecraft Username", value=username, inline=True)
                    embed.set_thumbnail(url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.default_avatar.url)
                    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
                    channel = interaction.guild.get_channel(int(logChannel))
                    if channel:
                        await channel.send(embed=embed)
                    await interaction.response.send_message("Your verification request has been sent for approval. Please wait on an authorized user to accept your verification.", ephemeral=True)
                    return
            await self.verifyUser(interaction, userGuildPrefix, userGuildRank, jsonData, username)
            await interaction.response.send_message(f"Discord user {interaction.user.name} (In-game username {username}) has successfully been verified.", ephemeral=True)
    
    @app_commands.command(description="Output your server's config list.")
    async def list_config(self, interaction: discord.Interaction):
        requiredRoleName = "Manage Permission"
        #logger.info(f"interaction.user.roles: {interaction.user.roles}")
        permission = 0
        for role in interaction.user.roles:
            #logger.info(f"role.name: {role.name}")
            if role.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return
        
        logger.info(f"Command /manage list_config was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        embed = await asyncio.to_thread(listConfig, interaction.guild_id, configTypes)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(description="Force-verify a discord user. Will bypass any verification checks.")
    @app_commands.describe(
        user_id='The user\'s discord account ID you want to verify',
        username='The user\'s ingame username you want to verify',
    )
    async def force_verify(self, interaction: discord.Interaction, user_id: str, username: str):
        requiredRoleName = "Manage Permission"
        #logger.info(f"interaction.user.roles: {interaction.user.roles}")
        permission = 0
        for role in interaction.user.roles:
            #logger.info(f"role.name: {role.name}")
            if role.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return
        
        logger.info(f"Command /manage force_verify was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).  Parameter user_id is: {user_id}, username is {username}.")
        user_id = int(user_id)
        success, r = await asyncio.to_thread(makeRequest, f"https://api.wynncraft.com/v3/player/{username}?fullResult")
        if not success: # Check if theyre a real user
            await interaction.response.send_message(f"{username} is not a user on wynncraft. Please recheck the username and try again.", ephemeral=True)
            return
        else: # theyre real
            jsonData = r.json()
            userGuildPrefix = jsonData.get("guild", {}).get("prefix")
            if userGuildPrefix:
                userGuildRank = jsonData["guild"]["rank"]
            await self.verifyUser(interaction, userGuildPrefix, userGuildRank, jsonData, username)
            await interaction.response.send_message(f"Discord user {interaction.user.name} (In-game username {username}) has successfully been verified.", ephemeral=True)
            
    @app_commands.command(description="Unverify a discord user. Will remove all roles that Dernal gave this user.")
    @app_commands.describe(
        user='The user\'s discord account account you want to unverify',
    )
    async def unverify(self, interaction: discord.Interaction, user: discord.Member):
        requiredRoleName = "Manage Permission"
        #logger.info(f"interaction.user.roles: {interaction.user.roles}")
        permission = 0
        for role in interaction.user.roles:
            #logger.info(f"role.name: {role.name}")
            if role.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return
        
        logger.info(f"Command /manage force_verify was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).  Parameter user is: {user}.")
        dbData = await asyncio.to_thread(getDatabaseData, interaction.guild_id)
    
        rolesToRemove = []
        for key, val in configTypes.items():
            if val == "role":
                roleIDStr = dbData.get(key)
                if roleIDStr:
                    role = interaction.guild.get_role(int(roleIDStr))
                    if role and role in user.roles:
                        rolesToRemove.append(role)
        
        if not rolesToRemove:
            await interaction.response.send_message(f"No eligible roles found on {user.mention} to remove.", ephemeral=True)
            return
        
        try:
            await user.remove_roles(*rolesToRemove)
            await interaction.response.send_message(f"Removed eligible roles from {user.mention}.", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to remove roles from user {user.id} in guild {interaction.guild_id}: {e}")
            await interaction.response.send_message(f"Failed to remove eligible roles from {user.mention}.", ephemeral=True)  

async def setup(bot):
    await bot.add_cog(Manage(bot))