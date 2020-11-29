import '../css/main.css';
import * as d3 from "d3";
import {React, Component} from 'react';
import Sectors from './Sectors';
import Index from './Index';
import Sector_Filter from './Sector_Filter';

export default class Container extends Component { 

  render() {
  return (
    <div className="main-container">
      <Sectors />
      <hr></hr>
      <Index />
      <hr></hr>
      <Sector_Filter />
    </div>
  );
  }
}