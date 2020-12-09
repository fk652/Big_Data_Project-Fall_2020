import '../css/main.css';
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend } from 'recharts';
import {React, Component} from 'react';
import data from '../datasets/S&P_500_Aggregate_Join_Oxford_JohnHopkins_News.json';

const formatNews = (arr) => {
  const headlines = arr.map((line) =>
    <p>{line}</p>
  );
  return (
    <div className="news-tooltip">{headlines}</div>
  );
};

const fixData = (data) => {
  let i = 0;
  for (i=0; i<data.length; i++){
    if (data[i]["Close"] === null){
      data[i]["Close"] = data[i-1]["Close"]
    }
  }
}

const getNews = (label) => {
  let i = 0;
  for (i=0; i<data.length; i++){
    if (data[i]["Date"] === label ){
        let news_variable = data[i]["News title--source"];
        let close = data[i]["Close"];
        let covid = data[i]["USA Covid Confirmed Cases"];
        let temp = news_variable.split("\t");
        let j = 0;
        let arr = []
        arr.push("Close: "+close);
        arr.push("Total Covid Cases: "+covid);
        for (j=0; j<4; j++){
            arr.push(temp[j])
        }
        let formatted_news = formatNews(arr);
        return formatted_news;
    }
  }
};

const CustomToolTip = ({ active, payload, label }) => {
  if (active) {
    return (
      <div className="custom-tooltip">
        <p className="label">{`${label}`}</p>
        <p className="intro">{getNews(label)}</p>
      </div>
    );
  }

  return null;
};

export default class Index extends Component { 
  render() {
  fixData(data);
  return (
    <div className="viz-container">
      <br></br>
      <LineChart
        width={1000}
        height={600}
        data={data}
        margin={{
          top: 5, right: 30, left: 20, bottom: 5,
        }}
      >
        <XAxis dataKey="Date" />
        <YAxis yAxisId="left" domain={[2000, 4000]} />
        <YAxis yAxisId="right" orientation="right" />
        <Tooltip content={<CustomToolTip />}/>
        <Legend />
        <Line yAxisId="left"  type="monotone" dataKey="Close" stroke="#8884d8" dot={false} />
        <Line yAxisId="right" type="monotone" dataKey="USA Covid Confirmed Cases" stroke="#82ca9d" dot={false}/>
      </LineChart>
    </div>
  );
}
}