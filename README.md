<h1 align="center">
  Dernal
</h1>

<h4 align="center">A lightweight and easy-to-use discord bot for everything in <a href="https://wynncraft.com/" target="_blank">Wynncraft</a>.</h4>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#how-to-use">How To Use</a> •
  <a href="#download">Download</a> •
  <a href="#credits">Credits</a> •
  <a href="#license">License</a>
</p>

![img](/lib/documents/example_image.png)

## Features

This is a quite versitile discord bot, with commands, like:

- **Detector**, a background task you can set up to passively track all things guild wars, including:
  - When and who takes your territory, at what time, and how long it lasted
  - When you take someone eles's territory, how long it lasted, and who it was taken from.
  - A somewhat accurate list of attacking members, and what world they were on.
  - Pings for when you lose a territory, and a customizable cooldown.
  - Compatability with running with multiple guilds at once.
  - A command which auto-fills what you are detecting, and allows you to remove it.
- **Guild Commands**, a very expansive amount of commands, like:
  - Overview: Allows you to get a quick overview of a guild, like level, online members, etc.
  - Inactivity: A command which shows you the last login date of every single user, and organizes them in pages.
  - Activity online_members: Shows a graph displaying the average amount of online members a guild has for the past day.
  - Activity territories: Shows a graph displaying the amount of territories a guild has for the past 3 days.
  - Activity total_members: Shows a graph displaying the total members a guild has for the past day.
  - Activity wars: Shows a graph displaying the total amount of wars a guild has done over the past 3 days.
  - Activity playtime: Shows the graph displaying the average amount of players online over the past day.
  - Activity xp: Shows a bar graph displaying the total xp a guild has every day, for the past 2 weeks.
  - Leaderboard xp: Shows a leaderboard of the top 10 guild's xp gained over the past 24 hours.
  - Leaderboard wars: Shows a leaderboard of the top 10 guild's war amount.
  - Leaderboard online_members: Shows a leaderboard of the top 10 guild's average amount of online players.
  - Leaderboard playtime: Shows a leaderboard of the top 10 guild's playtime percentage.
  - Leaderboard total_members: Shows a leaderboard of the top 10 guild's total members.
- **Giveaway Commands**
  - Allows configuiring what guild you want to track for weekly giveaways, and gives better odds to win the givaway based on serveral factors. For example, Having 300 minutes of playtime and 50m xp contributed over the past 7 days, along with weekly, will triple your chances of winning the giveaway versus someone who only did their weekly. All chances are hard-coded atm, however you can edit these as you like. This is primarily built for distributing tomes gained from weekly's, however this can be used for absolutely anything.
- **Player Commands**, a couple of commands, like:
  - Activity playtime: Shows the graph displaying the average amount minutes played every day for the past 2 weeks.
  - Activity contribution: Shows the graph displaying the amount of XP contribution to the player's guild over the past 2 weeks.
  - Activity mobs_killed: Shows the graph displaying the amount of mobs killed by the player over the past week.
  - Activity wars: Shows the graph displaying the amount of wars completed by the player ofer the past week.
  - Activity raids: Shows the graph displaying the amount of raids completed by the player over the past week.
  - Activity raids_pie: Shows a pie chart displaying the percentages of different types of raids done for the user.
  - Activity dungeons: Shows the graph displaying the amount of dungeons completed by the player over the past week.
  - Activity dungeons_pie: Shows a pie chart displaying the percentages of different types of dungeons done for the user.
  - leaderboard dungeons: Shows a top 10 list of the players with the most amount of dungeons completed.
  - leaderboard playtime: Shows a top 10 list of the players with the most amount of playtime total.
  - leaderboard pvp_kills: Shows a top 10 list of the players with the most amount of pvp kills..
  - leaderboard raids: Shows a top 10 list of the players with the most amount of raids completed.
  - leaderboard total_level: Shows a top 10 list of the players with the most amount of levels total.
- **Territory Maps**, two commands that can:
  - Map: Generates the current wynncraft territory map.
  - Heatmap: Generates a heatmap for all territories.
- **HQ Calculator**, a command that allows you to calculate the best HQ locations based on strength
  - Based on what your guild owns, or can be switched for the best hq in the whole map.
- **Overview**, a command that shows stats like:
  - Owner, Online, Total members, Wars and territory count, Top Season Ratings and Top Contributing Members.

> [!NOTE]  
> For self-hosting, you will need to run dernal with the activitySQL script to use /guild activity/leaderboard commands, /player activity commands, and /giveaway commands. Or you can join the [Dernal Support Discord](https://discord.gg/MHbMGjKdfe) discord to use our database.

## How To Use

To use this discord bot, you need to either pick the legacy webhook version, or the supported discord bot (recommended).

#### Discord bot

If you use our hosted bot, you can add it to your server via this [link](https://discord.com/oauth2/authorize?client_id=1270960638382051368). However, if you wish to self-host, follow these instructions:

To clone and run this discord bot, you'll need [Git](https://git-scm.com), [Python](https://www.python.org/downloads/) and [Pip](https://nodejs.org/en/download/) installed on your computer. From your command line:

```bash
# Clone this repository
$ git clone https://github.com/badpinghere/dernal

# Go into the repository
$ cd dernal

# Install dependencies
$ pip install -r requirements.txt

# Rename env file and add your bot token and other configs
$ mv .env.example .env
$ nano .env

# Run the app
$ python3 dernal.py

# Run the activity script
$ python3 lib/generateActivitySQL.py
```

#### Webhook Script

To clone and run this webhook script, you'll need [Git](https://git-scm.com), [Python](https://www.python.org/downloads/) and [Pip](https://nodejs.org/en/download/) installed on your computer. From your command line:

```bash
# Clone this repository
$ git clone https://github.com/badpinghere/dernal

# Go into the repository
$ cd dernal/legacy

# Install dependencies
$ pip install -r requirements_legacy.txt

# Rename and edit config_template.ini
$ mv config_template.ini config.ini
$ nano config.ini

# Run the app
$ python dernal_legacy.py
```

## Download

You can [download](https://github.com/BadPingHere/dernal/releases/latest) the latest installable version of dernal.py for Windows, macOS and Linux.

## Credits

This software was inspired or uses assets from:

- BoxFot
- [Nori](https://nori.fish)
- [Wynncraft-Territory-Info](https://github.com/jakematt123/Wynncraft-Territory-Info)
- [Wynntils](https://wynntils.com/)

## Bug Reporting

If you are having any issues, that being a command is running improperly, the grammar on an error is weird, or having issues getting the bot up and running, please do not hesitate to either contact me on discord, join the [Dernal Support Discord](https://discord.gg/MHbMGjKdfe) or submit an issue on [Github](https://github.com/BadPingHere/dernal/issues).

## License

GPLv3

---

> GitHub [@BadPingHere](https://github.com/BadPingHere)&nbsp;&middot;&nbsp;
> Discord [BadPingHere](https://discord.com/users/736028271153512489)
