import './App.css';
import Header from './js/Header';
import React, { Component } from 'react';
import Container from './js/Container';

class App extends Component { 
  render() {
  return (
    <div className="App">
      <Header/>
      <center>
      <Container />
      </center>
    </div>
  );
  }
}

export default App;
