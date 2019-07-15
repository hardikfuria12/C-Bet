# C-Bet: Beat the odds

<div style="text-align:center; margin: 50px 0"><img src ="/docs/img/icon.png" height="200"/></div>


## Project Background

Betfair is the largest Online Sports Betting Exchange. Its turnover was more than 700 million dollars in 2018. Betfair network manages more than two hundred thousand transaction per second. Betfair Historical Data Service provides Betfair Exchange Data which can be used to analyze market trends, back test betting strategy before testing out in the real-case life scenario. Since data relevancy is highly coupled with the background information about the sports, it lets user deep dive in to the sports and the different betting markets associated with it.  

## Real Life Scenario

<div style="text-align:center; margin: 50px 0"><img src ="/docs/img/betfair-ui.png"/></div>

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

If user had bet 100 dollars on **runner** - NYC for *market* - "Match Odds" at *betting rate* of 2.0.
User will get back 2.0*100 = 200$.
If user had bet on *runner* Draw or Portland Timbers, he would lost his 100 dollars.

Betting rate is *dynamic* in nature, it keeps on changing till the **event** has officially ended.
This article gives a much detailed explaination of how the betting rates are given out and adjusted by the the bookeepers. 
So the user in the above case is unsure whether to place a bet at the given rate or wait for a better rate to be offered.

Generally, if the probability of given result is higher then the bettting rate for it would be lower. Like in the above cae NYC is given betting rate of 2.0 because NYC is a very stong Soccer Team and in almost all the matches NYC plays, they are always the favorites, thereby the betting rate in favor of NYC is generally low.

## Problem Statement

To find when was the maximum and most favorable odds given, by, converting 100 gigabytes of unstructured, messy time-stamped historical data to a structured and easy to use Data Repository.
Having a structured data repository for a complex system enables faster analysis and quick decision making.  

## Pipeline

<div style="text-align:center; margin: 50px 0"><img src ="/docs/img/pipeline.png" height="200"/></div>

## Authors

* **Hardik Furia** - [GitHub: hardikfuria12](https://github.com/hardikfuria12/)

## Acknowledgments

This project acknowledges [Insight Data Science's Data Engineering Program](https://www.insightdataengineering.com/) for the guidance. 