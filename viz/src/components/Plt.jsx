import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import Chart from 'chart.js/auto';

import Accordion from '@mui/material/Accordion';
import AccordionSummary from '@mui/material/AccordionSummary';
import AccordionDetails from '@mui/material/AccordionDetails';
import Typography from '@mui/material/Typography';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

const Plt = ({ restProxyUrl, topic }) => {
  const [messages, setMessages] = useState([]);
  const [vizKey, setVizKey] = useState('');
  const chartRef = useRef(null);
  const distributionChartRef = useRef(null);
  const messageListRef = useRef([]);

  const consumerGroup = 'frontend';
  const consumerInstance = 'viz-react';

  // Fetch messages
  const fetchMessages = async () => {
    try {

      const response = await axios.get(
        `http://${restProxyUrl}/consumers/${consumerGroup}/instances/${consumerInstance}/records`,
        {
          headers: {
            'Accept': 'application/vnd.kafka.json.v2+json',
          },
        }
      );
      setMessages((prevMessages) => [...prevMessages, ...response.data]);
    } catch (error) {
      console.error('Error fetching messages:', error);
    }
  };

  const subscribeToTopic = async () => {
    try {
      await axios.post(
        `http://${restProxyUrl}/consumers/${consumerGroup}/instances/${consumerInstance}/subscription`,
        {
          topics: [topic],
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

  const handleKeySubmit = (e) => {
    e.preventDefault();
    // Read the form data
    const form = e.target;
    const formData = new FormData(form);
    const formJson = Object.fromEntries(formData.entries());
    const vizKey = formJson.vizKey;
    setVizKey(vizKey);
    
    // Reset charts
    if (chartRef.current) {
      chartRef.current = null;
    }
    if (distributionChartRef.current) {
      distributionChartRef.current = null;
    }
  };

  const createGraphs = () => {
    try {
      const canvas = document.getElementById('graphCanvas');
      const ctx = canvas.getContext('2d');

      chartRef.current = new Chart(ctx, {
        type: 'line',
        data: {
          labels: [],
          datasets: [
            {
              label: 'Messages',
              data: [],
              backgroundColor: 'rgba(75, 192, 192, 0.2)',
              borderColor: 'rgba(75, 192, 192, 1)',
              borderWidth: 1,
            },
          ],
        },
        options: {
          responsive: true,
          scales: {
            x: {
              display: true,
            },
            y: {
              display: true,
              beginAtZero: true,
            },
          },
        },
      });

      const distributionCanvas = document.getElementById('distributionCanvas');
      const distributionCtx = distributionCanvas.getContext('2d');

      distributionChartRef.current = new Chart(distributionCtx, {
        type: 'bar',
        data: {
          labels: [],
          datasets: [
            {
              label: 'Value Distribution',
              data: [],
              backgroundColor: 'rgba(75, 192, 192, 0.2)',
              borderColor: 'rgba(75, 192, 192, 1)',
              borderWidth: 1,
            },
          ],
        },
        options: {
          responsive: true,
          scales: {
            x: {
              display: true,
            },
            y: {
              display: true,
              beginAtZero: true,
            },
          },
        },
      });
    } catch (error) {
      console.error('Error creating graphs:', error);
    }
  };


  // Update charts on message change
  useEffect(() => {
    if (document.getElementById('graphCanvas')) {
      if (chartRef.current && distributionChartRef.current) {
        // Get value of given vizKey from messages
        const values = messages.map((message) => message.value[vizKey]);

        // Update chart with values
        chartRef.current.data.labels = messages.map((message) => message.offset);
        chartRef.current.data.datasets[0].data = values;
        chartRef.current.update();

        // Update chart with value distribution
        const distributionData = calculateValueDistribution(values);
        distributionChartRef.current.data.labels = distributionData.labels;
        distributionChartRef.current.data.datasets[0].data = distributionData.values;
        distributionChartRef.current.update();
      } else {
        createGraphs();
      }
    }
    // Set messageListRef to contain all messages
    messageListRef.current = messages;
  }, [messages]);

  // Calculate value distribution
  const calculateValueDistribution = (values) => {

    const binSize = (Math.max(...values) - Math.min(...values)) / 10;
    const binCounts = Array.from({ length: 10 }, () => 0);

    // Calculate the counts for each bin
    values.forEach((value) => {
      const binIndex = Math.floor((value - Math.min(...values)) / binSize);
      binCounts[binIndex]++;
    });

    // Prepare the labels for the chart
    const labels = binCounts.map((count, index) => {
      const binStart = Math.min(...values) + index * binSize;
      const binEnd = binStart + binSize;
      return `${binStart.toFixed(2)} - ${binEnd.toFixed(2)}`;
    });

    return { labels, values: binCounts };
  };

  // Poll for new messages
  useEffect(() => {
    const intervalId = setInterval(fetchMessages, 1000);
    subscribeToTopic();

    return () => {
      clearInterval(intervalId);
    };
  }, []);

  return (
    <div>
      <h3 class="text-xl mb-4">{topic}</h3>
      <h3 class="text-xl mb-4"></h3>

      <Accordion>
        <AccordionSummary
          expandIcon={<ExpandMoreIcon />}
          aria-controls="panel1a-content"
          id="panel1a-header"
        >
          <Typography>Current messages</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <div class="max-h-72 overflow-auto">
            {messageListRef.current.slice(0).reverse().map((line, index) => (
              <p key={index}>{JSON.stringify(line.value)}</p>
            ))}
          </div>
        </AccordionDetails>
      </Accordion>

      <form onSubmit={handleKeySubmit} class="py-3">
        <label>Enter specific key to visualize:</label>
        <br />
        <input type="text" name="vizKey" class="p-2 rounded mr-2" />
        <button type="submit">Submit</button>
      </form>

      {vizKey && (
        <>
          <Accordion>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="panel2a-content"
              id="panel2a-header"
            >
              <Typography>Values Line-Plot</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <canvas id="graphCanvas" />
            </AccordionDetails>
          </Accordion>
          <Accordion>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="panel2a-content"
              id="panel2a-header"
            >
              <Typography>Value Distribution</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <canvas id="distributionCanvas" />
            </AccordionDetails>
          </Accordion>
        </>
      )}
    </div>
  );
};

export default Plt;
