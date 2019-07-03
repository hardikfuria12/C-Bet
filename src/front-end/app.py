# standard library
import os

# dash libs
import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import plotly.figure_factory as ff
import plotly.graph_objs as go

# pydata stack
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

# set params
myDB = URL(drivername='mysql+pymysql', username='root',
                   password='12345678',
                   host='cbet.c0jjgccwckol.us-east-1.rds.amazonaws.com',
                   database='cbet',
                   query={'read_default_file': '~/.my.cnf'})
conn = create_engine(myDB)

# conn = create_engine(os.environ['DB_URI'])


###########################
# Data Manipulation / Model
###########################

def fetch_data(q):
    result = pd.read_sql(
        sql=q,
        con=conn
    )
    return result


def get_team_list():  # rename to get_teams
    '''Returns the list of divisions that are stored in the database'''

    division_query = (
        f'''
        select distinct instancename from marketinstance where markettype="match_odds" order by instancename
        '''
    )
    divisions = fetch_data(division_query)
    divisions = list(divisions['instancename'])
    return divisions


def get_seasons(division):
    '''Returns the seasons of the datbase store'''

    seasons_query = (
        f'''
        SELECT a.eventname as eventname,a.eventdate as eventdate,
        b.markettype as markettype,b.result as result,c.instanceid as instanceid,b.instancename as instancename,
        c.rate as rate,c.timestamp as timestamp
        FROM event a 
        JOIN marketinstance b on b.eventid=a.eventid
        JOIN instancerate c on b.instanceid=c.instanceid
        JOIN (
                SELECT b.instanceid as instanceid,MAX(c.rate) as rate 
                from event a 
                join marketinstance b on b.eventid=a.eventid 
                join instancerate c on b.instanceid=c.instanceid 
                where (a.eventname like "{division}%%"or a.eventname like "%%{division}") 
                group by c.instanceid
                ) d on c.instanceid=d.instanceid

        where (a.eventname like "{division}%%" or a.eventname like "%%{division}")
        and c.rate=d.rate and c.instanceid=d.instanceid order by instanceid;

        '''
    )
    print(seasons_query,'seasonQUERY')
    seasons = fetch_data(seasons_query)
    print()
    seasons = list(seasons['instancename'].unique())
    print(seasons)

    return seasons




def get_match_results(division, season):
    '''Returns match results for the selected prompts'''

    results_query = (
        f'''
        SELECT a.eventname as eventname,a.eventdate as eventdate,b.instancename as instancename,
        b.markettype as markettype,b.result as result,c.instanceid as instanceid,
        c.rate as rate,c.timestamp as timestamp
        FROM event a 
        JOIN marketinstance b on b.eventid=a.eventid
        JOIN instancerate c on b.instanceid=c.instanceid
        JOIN (
                SELECT b.instanceid as instanceid,MAX(c.rate) as rate 
                from event a 
                join marketinstance b on b.eventid=a.eventid 
                join instancerate c on b.instanceid=c.instanceid 
                where (a.eventname like "{division}%%"or a.eventname like "%%{division}") and c.rate<500
                group by c.instanceid
                ) d on c.instanceid=d.instanceid

        where (a.eventname like "{division}%%" or a.eventname like "%%{division}")
        and c.rate=d.rate and c.instanceid=d.instanceid and b.instancename="{season}" order by timestamp;

        '''
    )
    match_results = fetch_data(results_query)
    return match_results




def draw_season_points_graph(results):
    print('draw_season',results)
    dates = results['timestamp'].tolist()
    points = results['rate'].tolist()
    print(type(dates[0]))
    print(points)

    figure = go.Figure(
        data=[
            go.Scatter(x=points, y=points, mode='lines+markers')
        ],
        layout=go.Layout(
            title='Points Accumulation',
            showlegend=False
        )
    )
    # print(type(figure))
    return figure


#########################
# Dashboard Layout / View
#########################



def onLoad_division_options():
    '''Actions to perform upon initial page load'''

    division_options = (
        [{'label': division, 'value': division}
         for division in get_team_list()]
    )
    return division_options


# Set up Dashboard and create layout
app = dash.Dash()
app.css.append_css({
    "external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"
})


app.layout = html.Div([

    # Page Header
    html.Div([
        html.H1('CBET Analysis')
    ]),

    # Dropdown Grid
    html.Div([
        html.Div([
            # Select Division Dropdown
            html.Div([
                html.Div('Select Team', className='three columns'),
                html.Div(dcc.Dropdown(id='division-selector',
                                      options=onLoad_division_options()),
                         className='nine columns')
            ]),

            # Select Season Dropdown
            html.Div([
                html.Div('Select Market Type', className='three columns'),
                html.Div(dcc.Dropdown(id='season-selector'),
                         className='nine columns')
            ]),
        ], className='six columns'),

        # Empty
        html.Div(className='six columns'),
    ], className='twleve columns'),

    # Match Results Grid
    html.Div([
        html.Div([
            dcc.Graph(id='season-graph')
        ], className='six columns')
    ]),
])


#############################################
# Interaction Between Components / Controller
#############################################

# Load Seasons in Dropdown
@app.callback(
    Output(component_id='season-selector', component_property='options'),
    [
        Input(component_id='division-selector', component_property='value')
    ]
)
def populate_season_selector(division):
    print("Hardik",division)
    seasons = get_seasons(division)
    return [
        {'label': season, 'value': season}
        for season in seasons
    ]


# # Load Teams into dropdown
# @app.callback(
#     Output(component_id='team-selector', component_property='options'),
#     [
#         Input(component_id='division-selector', component_property='value'),
#         Input(component_id='season-selector', component_property='value')
#     ]
# )
# def populate_team_selector(division, season):
#     teams = get_teams(division, season)
#     return [
#         {'label': team, 'value': team}
#         for team in teams
#     ]



# Update Season Point Graph
@app.callback(
    Output('season-graph', 'figure'),
    [
        Input(component_id='division-selector', component_property='value'),
        Input(component_id='season-selector', component_property='value')
    ]
)
def load_season_points_graph(division, season):
    results = get_match_results(division, season)
    print(results)
    trace_high = go.Scatter(
    x=results['timestamp'],
    y=results['rate'],
    # z=results['eventname'],
    # l=results['eventdate'],
    # hovertemplate='<i>Rate</i>: $%{y:.2f}'
    #               '<br><b>Event:</b>: %{z}<br>'
    #               '<br><b>Date:</b>: %{l}<br>',
    text=results['eventname'],
    # text2=results['eventdate'],
    # hoverinfo='y',
    hoverinfo='text',

    name = "Hmm",
    line = dict(color = '#17BECF'),
    opacity = 0.8)
    data=[trace_high]
    fig=dict(data=data)
    return fig







# start Flask server
if __name__ == '__main__':
    app.run_server(
        debug=True,
        host='0.0.0.0',
        port=8050
    )







































# #######
# # This script will make regular API calls to http://data-live.flightradar24.com
# # to obtain updated total worldwide flights data.
# # ** This version continuously updates the number of flights worldwide,
# #    AND GRAPHS THOSE RESULTS OVER TIME! **
# ######
# import dash
# import dash_core_components as dcc
# import dash_html_components as html
# from dash.dependencies import Input, Output
# import plotly.graph_objs as go
# import requests
#
# app = dash.Dash()
#
# app.layout = html.Div([
#     html.Div([
#         html.Iframe(src = 'https://www.flightradar24.com', height = 500, width = 1200)
#     ]),
#
#     html.Div([
#     html.Pre(
#         id='counter_text',
#         children='Active flights worldwide:'
#     ),
#     dcc.Graph(id='live-update-graph',style={'width':1200}),
#     dcc.Interval(
#         id='interval-component',
#         interval=6000, # 6000 milliseconds = 6 seconds
#         n_intervals=0
#     )])
# ])
# counter_list = []
#
# @app.callback(Output('counter_text', 'children'),
#               [Input('interval-component', 'n_intervals')])
# def update_layout(n):
#     url = "https://data-live.flightradar24.com/zones/fcgi/feed.js?faa=1\
#            &mlat=1&flarm=1&adsb=1&gnd=1&air=1&vehicles=1&estimated=1&stats=1"
#     # A fake header is necessary to access the site:
#     res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
#     data = res.json()
#     counter = 0
#     for element in data["stats"]["total"]:
#         counter += data["stats"]["total"][element]
#     counter_list.append(counter)
#
#     return 'Active flights worldwide: {}'.format(counter)
#
#
# @app.callback(Output('live-update-graph','figure'),
#               [Input('interval-component', 'n_intervals')])
#
# def update_graph(n):
#
#     fig = go.Figure(
#         data = [go.Scatter(
#         x = list(range(len(counter_list))),
#         y = counter_list,
#         mode='lines+markers'
#         )])
#     return fig
#
#
#
# if __name__ == '__main__':
#     app.run_server()