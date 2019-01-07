import React, { Component } from 'react';
import styled from 'styled-components';

const TopicHome = styled.div`
  display: flex;
  flex-direction: row;
`;

const TopicCard = styled.div`
  text-align: left;
  border: solid;
`;

const TopicName = styled.h1`
  font-size: 1.5em;
  color: palevioletred;
`;

const TopicCount = styled.h1`
  font-size: 1em; 
  color: palevioletred;
`;

class App extends Component {
  
  render() {
    return (
      <TopicHome>
        <TopicCard>
          <TopicName>
            User Email Updated
          </TopicName>
          <TopicCount>
            50
          </TopicCount>
          <a
            className="Topic Details"
            target="_blk"
            rel="noopener noreferrer"
          >
            More details
          </a>
        </TopicCard>
      </TopicHome>
    );
  }
}

export default App;
