#!/usr/bin/env python

from flask import Flask, make_response, template_rendered, templating, render_template
import redis
import pandas as pd

app     =   Flask(__name__)
db      =   redis.Redis(host="localhost", port=6379, db=0)
smurfs  =   [ str(i).zfill(2) for i in range(1, 22+1) ]

@app.route('/<smurf>')
def show_tables(smurf=None):
    if smurf not in smurfs: 
        return "smurf be all smashed n stuff"
    else:
        d = pd.read_msgpack(db.get(smurf))
        tables = [ d.to_html(classes='table', index=True, escape=False) ]
        return render_template('smurf.html', tables=tables, smurf=smurf)

app.run(debug=False)
