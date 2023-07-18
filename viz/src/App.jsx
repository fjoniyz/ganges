import React, { useState } from 'react';
import axios from 'axios';

import Plt from './components/Plt';

import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemText from '@mui/material/ListItemText';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemButton from '@mui/material/ListItemButton';

import LabelIcon from '@mui/icons-material/Label';

const App = () => {
  const [topics, setTopics] = useState([]);
  const [messages, setMessages] = useState([]);
  const [subscribedTopics, setsubscribedTopics] = useState([]);
  const [restProxyUrl, setRestProxyUrl] = useState('');
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  let fetchIntervalId = null;

  const fetchTopics = async (proxyUrl) => {
    let retrievedTopics = [];
    while (retrievedTopics.length === 0) {
      try {
        const response = await axios.get(`http://${proxyUrl}/topics`);
        retrievedTopics = response.data;
        setTopics(response.data);
      } catch (error) {
        console.error('Error fetching topics:', error);
      }
      await sleep(2000);
    }
  };

  const handleUrlSubmit = (e) => {
    e.preventDefault();
    // Read the form data
    const form = e.target;
    const formData = new FormData(form);
    const formJson = Object.fromEntries(formData.entries());
    const proxyUrl = formJson.proxyUrl;
    setRestProxyUrl(proxyUrl);
    fetchTopics(proxyUrl);
  };

  const setConsumer = async (proxyUrl) => {
    // TODO: Check if consumer already exists
    // If not, create a new consumer
    try {
      await axios.post(`http://${proxyUrl}/consumers/frontend`, {
        "name": "frontend",
        "format": "json",
        "auto.offset.reset": "earliest",
      },
        {
          headers: {
            'Content-Type': 'application/vnd.kafka.v2+json',
          },
        });

    } catch (error) {
      console.error('Error setting consumer:', error);
    }
  };

  const subscribeToTopic = async (topics) => {
    try {
      await axios.post(
        `http://${restProxyUrl}/consumers/frontend/instances/frontend/subscription`,
        {
          topics: topics,
        },
        {
          headers: {
            'Content-Type': 'application/vnd.kafka.v2+json',
          },
        }
      );
    } catch (error) {
      console.error('Error subscribing to topic:', error);
    }
  };

  const fetchMessages = async () => {
    try {

      const response = await axios.get(
        `http://${restProxyUrl}/consumers/frontend/instances/frontend/records`,
        {
          headers: {
            'Accept': 'application/vnd.kafka.json.v2+json',
            'Content-Type': 'application/vnd.kafka.json.v2+json',
          },
        }
      );
      return response.data;
    } catch (error) {
      console.error('Error fetching messages:', error);
    }
  };

  const handleSubscribe = async (selectedTopic) => {
    setConsumer(restProxyUrl);
    setsubscribedTopics([...subscribedTopics, selectedTopic]);
    subscribeToTopic([...subscribedTopics, selectedTopic]);
    fetchIntervalId = setInterval(async () => {
      const messageResponse = await fetchMessages();
      if (messageResponse) setMessages((prevMessages) => [...prevMessages, ...messageResponse]);
    }, 1000);
  };

  return (
    <div class="m-10">
      <h1 class="mb-10 text-5xl">Kafka Visualizer</h1>
      {!restProxyUrl && (
        <form onSubmit={handleUrlSubmit}>
          <label>REST Proxy URL:</label>
          <br />
          <input
            type="text"
            name="proxyUrl"
            defaultValue="localhost:8082"
            class="p-2 rounded mr-2"
          />
          <button type="submit">Submit</button>
        </form>
      )}
      {restProxyUrl && topics.length === 0 && (
        <p>Loading topics...</p>
      )}
      {topics.length > 0 && (
        <>
          <h2 class="text-2xl my-4">Available Topics:</h2>
          <List>
            {topics.map((topic) => (
              <ListItem disablePadding key={topic}>
                <ListItemButton
                  onClick={() => handleSubscribe(topic)}
                  // if topic in subscribedTopics, disable button
                  disabled={subscribedTopics.includes(topic)}
                >
                  <ListItemIcon>
                    <LabelIcon />
                  </ListItemIcon>
                  <ListItemText
                    primary={topic}
                  />
                </ListItemButton>
              </ListItem>
            ))}
          </List>
          {subscribedTopics.length > 0 && (
            <>
              <h2 class="text-2xl my-4">Subscribed Topics:</h2>
              <ul>
                {subscribedTopics.map((topic) => (
                  <li class="w-[90vw] border rounded p-5" key={topic}>
                    <Plt key={topic} topic={topic} restProxyUrl={restProxyUrl} messages={messages} />
                  </li>
                ))}
              </ul>
            </>
          )}
        </>
      )}
    </div>
  );
};

export default App;
