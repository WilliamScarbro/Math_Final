import pandas as pd
import sqlite3
import os

def read_cities():
    data = pd.read_csv("./cities.csv",sep=';')
    df = pd.DataFrame(data,columns=['City','Rank','State','Growth From 2000 to 2013','Population','Coordinates'])
    states = set()
    for index, row in df.iterrows():
        states.add(row["State"].replace(' ','_'))
    return df,states

def load_tweets(df,keyword,start_date,radius):
    for index, row in df.iterrows():
        state = row['State'].replace(' ','_')
        if keyword==None:
            params = '',state,start_date,row['Coordinates'],radius
        else:
            params = '-s {}'.format(keyword),state,start_date,row['Coordinates'],radius
        command = 'twint {} --database "./state_tweets/{}.db" --since "{} 00:00:00"  -g="{},{}"'.format(*params)
        if (' ' in row['State']):
            os.system(command)

def coalate_tweets(states):
    con = sqlite3.connect("Coalated.db")
    cur = con.cursor()
    try:
        cur.execute("CREATE TABLE tweets ( tweet text not null, date text not null, time text not null, state text not null);")
    except:
        pass
    for state in states:
        #read from state database into coalated
        conn = sqlite3.connect("./state_tweets/{}.db".format(state))
        c = conn.cursor()
        try:
            for row in c.execute('SELECT tweet,date,time FROM tweets;'):
                fields = (row[0],row[1],row[2],state)
                command = "INSERT INTO tweets (tweet,date,time,state) VALUES (?,?,?,?);"
                cur.execute(command,fields)
                print("executed {}".format(command))
        except Exception as e:
            print(e)
            print("no tweets found for {}.db".format(state))
    con.commit()
    con.close()

if __name__=="__main__":
    start_date = '2020-01-01'
    radius = '1km'
    keyword = 'covid' #not case sensitive, use None for all tweets
    df,states = read_cities()
    load_tweets(df,keyword,start_date,radius)
    coalate_tweets(states)



