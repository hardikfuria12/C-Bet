# standard library
import os
import datetime
import time

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
    seasons = list(seasons['markettype'].unique())
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
        and c.rate=d.rate and c.instanceid=d.instanceid and b.markettype="{season}" order by timestamp;

        '''
    )
    match_results = fetch_data(results_query)
    return match_results

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



external_scripts = [
    'https://www.google-analytics.com/analytics.js',
    {'src': 'https://cdn.polyfill.io/v2/polyfill.min.js'},
    {
        'src': 'https://cdnjs.cloudflare.com/ajax/libs/lodash.js/4.17.10/lodash.core.js',
        'integrity': 'sha256-Qqd/EfdABZUcAxjOkMi8eGEivtdTkh3b65xCZL4qAQA=',
        'crossorigin': 'anonymous'
    }
]

# external CSS stylesheets
external_stylesheets = [
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
    {
        'href': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css',
        'rel': 'stylesheet',
        'integrity': 'sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO',
        'crossorigin': 'anonymous'
    }
]


app = dash.Dash(__name__,
                external_scripts=external_scripts,
                external_stylesheets=external_stylesheets)

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
                html.Div('Select Team', className='six columns'),
                html.Div(dcc.Dropdown(id='division-selector',
                                      options=onLoad_division_options()),
                         className='twelve columns')
            ]),

            # Select Season Dropdown
            html.Div([
                html.Div('Select Market Type', className='six columns'),
                html.Div(dcc.Dropdown(id='season-selector'),
                         className='twelve columns')
            ]),
        ]),

        # Empty
    ]),

    # Match Results Grid
    html.Div([
        html.Div([
            dcc.Graph(id='season-graph')
        ], className='twelve columns'),
    html.Div(id='text-content',className='twelve columns'),
    ])
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
    instance_list=list(results['instancename'].unique())
    traces=[]
    for name in instance_list:
        temp_df=results[results['instancename']==name]
        traces=traces+[go.Scatter(
        x=temp_df['timestamp'],
        y=temp_df['rate'],
        mode='markers+lines',
        customdata=temp_df,
        name=name)]
    fig=go.Figure(data=traces)
    return fig


@app.callback(
    Output('text-content','children'),
    [Input('season-graph','hoverData')]
)
def update_text(hoverData):
    get_info=hoverData['points'][0]['customdata']
    print(get_info)
    event_str=datetime.datetime.strptime(get_info[1].split(' ')[0],"%Y-%m-%d")
    event_str=event_str.strftime("%d %B, %Y")
    cause=str(get_info[4])
    print(cause)
    if cause=="WINNER":
        cause="winning"
    else:
        cause="losing"
    return html.P(
        'Maximum Rate of {} was given during : {} that happened on {} for a {} cause'
        .format(
            get_info[6],
            get_info[0],
            event_str,
            cause
        )
    )

# start Flask server
if __name__ == '__main__':
    app.run_server(
        debug=True,
        host='0.0.0.0',
        port=8050
    )