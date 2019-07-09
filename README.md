# C-Bet: Beat the odds

<div style="text-align:center; margin: 50px 0"><img src ="/docs/img/icon.png" height="200"/></div>

## Table of Contents
1. [Problem](README.md#problem)
1. [Basic Strategy](README.md#basic-strategy)
1. [Running Instructions](README.md#running-instructions)
1. [Demo](README.md#demo)
1. [Assumptions](README.md#assumptions)
1. [Files in Repo](README.md#files-in-repo)
1. [Encryption](README.md#encryption)
1. [Scalability](README.md#scalability)
1. [Future Work](README.md#future-work)
1. [Contact Information](README.md#contact-information)

## Background


Betfair is the largest Online Sports Betting Exchange. Its turnover was more than 700 million dollars in 2018. Betfair manages more than 200 thousand transacton per sec.    which provides users with betting odds for all the sporting events happening around the world.
<div style="text-align:center; margin: 50px 0"><img src ="/docs/img/betfair-ui.png" height="200"/></div>

Above figure, represents the User-Interface of Betfair. Where each sporting match between two teams is considered as an **'Event'** (highlighted by the Red Arrow).
For a given event there are various **'Markets'** ( highlighted with blue arrow ) Each Market has various contenders called **'Runners'**. On the completion of an Event there would be just one winner from all the runners that are there in a given market. 
**Betting Rates** for all the different **Runners** is marked in Green.
 
Lets learn with example.

From the figure above we select,

Event - **New York City** vs **Portland Timbers**

Market - **Match Odds**

No of Runners - 3

Runners - New York City (NYC), Portland Timbers (PT), Draw(TD).

Betting Rate - 2.0 (NYC), 4.0 (PT), 4.1 (TD).

Event Result - **NYC** won the match.

If user had bet 100 dollars on *runner* - NYC for *market* - "Match Odds" at *betting rate* of 2.0.
User will get back 2.0*100 = 200$.

If user had bet on *runner* Draw or Portland Timbers, he would lost his 100 dollars.

## Pipeline

<div style="text-align:center; margin: 50px 0"><img src ="/docs/img/pipeline.png" height="200"/></div>

## Authors

* **Hardik Furia** - [GitHub: hardikfuria12](https://github.com/hardikfuria12/)

## Acknowledgments

This project acknowledges [Insight Data Science's Data Engineering Program](https://www.insightdataengineering.com/) for the guidance. 