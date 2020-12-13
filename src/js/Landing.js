import '../css/main.css';
import {React, Component} from 'react';
import landing_image from '../landing-image.png';

export default class Container extends Component { 

    render() {
    return (
      <div className="landing-container">
        <img alt="" className="image-class" src={landing_image} height="350" width="auto" />
        <br></br>
        <br></br>
        <a href="https://github.com/fk652/Big_Data_Project-Fall_2020/">
            <div className="github-button">
                View on GitHub
            </div>
        </a>
        <p className="abstract-class">
            The COVID-19 outbreak began in Wuhan, China in November 2019. It has since spread rapidly leading <br></br>
            numerous governments have implement a range of policy responses such as the enforcement of social distancing, <br></br>
            the obligatory wearing of masks in public, limitations on internal and international travel, border closures, <br></br>
            stay-at-home requirements, and limitations on gathering size in order to flatten the death and infection rates. <br></br>
            Despite this terrifying human and economic impact, financial markets have fared quite well. The S&P 500, an indicator <br></br>
            used to gauge the overall health of large American companies, has gained 13.4% on a compound basis since November 15, 2019. <br></br>
            In this data analysis, we aimed to understand how the S&P500 was able to move to all-time highs despite the the consequences <br></br>
            caused by the coronavirus. 
            <hr></hr>
            By Parth Merchant, Nathalie Darvas & Fahim Khan
        </p>
      </div>
    );
    }
  }