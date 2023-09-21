import '../css/main.css';
import {BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ReferenceLine } from 'recharts';
import {React, Component} from 'react';
import aggregate_data from '../datasets/GainsDrops.json';

const formatNews = (arr) => {
  const headlines = arr.map((line) =>
    <p>{line}</p>
  );
  return (
    <div className="news-tooltip">{headlines}</div>
  );
};

const getNews = (label) => {
  let i = 0;
  for (i=0; i<aggregate_data.length; i++){
    if (aggregate_data[i]["Date"] === label ){
        let news_variable = aggregate_data[i]["News title--source"];
        let temp = news_variable.split("\t");
        let j = 0;
        let arr = []
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
        <p className="label">{`${label}\nLog Return: ${payload[0].value}`}</p>
        <p className="intro">{getNews(label)}</p>
      </div>
    );
  }

  return null;
};

export default class AggregateGainsDrops extends Component { 

  render() {
  return (
    <div className="viz-container">
      <br></br>
      <BarChart
          width={700}
          height={500}
          data={aggregate_data}
          margin={{
            top: 5, right: 30, left: 20, bottom: 5,
          }}
        >
          <XAxis dataKey="Date" />
          <YAxis />
          <Tooltip content={<CustomToolTip/>}/>
          <Legend />
          <ReferenceLine y={0} stroke="#000" />
          <Bar dataKey="Log Return" fill="#E1B505" />
        </BarChart>
    </div>
  );
  }
}