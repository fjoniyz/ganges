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
  const [progCheck, setprogCheck] = useState(false);
  const [progResult, setProgResult] = useState('');
  const [messageIdKey, setMessageIdKey] = useState('id');
  const [comparedTopic, setCompareTopic] = useState('')
  const chartRef = useRef(null);
  const compareChartRef = useRef(null);
  const distributionChartRef = useRef(null);
  const messageListRef = useRef([]);

  const consumerGroup = 'frontend';
  const consumerInstance = 'viz-react';
  let porgPollIntervalId = null;

  const subscribeToTopic = async (topic) => {
    try {
      await axios.post(
        `http://${restProxyUrl}/consumers/${consumerGroup}/instances/${consumerInstance}/subscription`,
        {
          topics: topic,
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
        `http://${restProxyUrl}/consumers/${consumerGroup}/instances/${consumerInstance}/records`,
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

  const fetchProgResults = async (progApiUrl, topic) => {
    try {
      const response = await axios.get(`http://${progApiUrl}/prognose?topic=${topic}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching prognose data:', error);
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

  const handleProgCheckChange = () => {
    setprogCheck(!progCheck);
    if (!progCheck) {
      porgPollIntervalId = setInterval(async () => {
        const messageResponse = await fetchProgResults("localhost:8080", topic);
        if (messageResponse) setProgResult(messageResponse);
      }, 1000);
    } else {
      clearInterval(porgPollIntervalId);
    }
  };

  const handleCompareSubmit = async (e) => {
    e.preventDefault();
    const form = e.target;
    const formData = new FormData(form);
    const formJson = Object.fromEntries(formData.entries());
    const compareTopic = formJson.compareTopic;
    setCompareTopic(compareTopic);
    setMessageIdKey(formJson.idKey);

    subscribeToTopic([topic, compareTopic]);

    if (compareChartRef.current) {
      compareChartRef.current = null;
    }
  };

  const createValueGraph = () => {
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
    } catch (error) {
      console.error('Error creating graphs:', error);
    }
  };

  const createDistributionGraph = () => {
    try {
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

  const createCompareGraph = () => {
    try {
      const canvas = document.getElementById('compareCanvas');
      const ctx = canvas.getContext('2d');

      compareChartRef.current = new Chart(ctx, {
        type: 'bar',
        data: {
          labels: [],
          datasets: [
            {
              label: 'Noise',
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

  const compareValues = () => {
    const messagesA = messages.filter(message => message.topic === topic)
    const messagesB = messages.filter(message => message.topic === comparedTopic)

    const values = messagesA.filter((mA) => messagesB.find(mB => mB.value[messageIdKey] === mA.value[messageIdKey]))
      .map((mA) => {
        const mB = messagesB.find(mB => mB.value[messageIdKey] === mA.value[messageIdKey]);
        return (mB.value[vizKey] - mA.value[vizKey]);
      });

    return calculateValueDistribution(values);
  };


  // Update charts on message change
  useEffect(() => {
    if (document.getElementById('graphCanvas')) {
      if (chartRef.current) {
        // Get value of given vizKey from messages
        const values = messages.filter(message => message.topic === topic).map(message => message.value[vizKey]);

        // Update chart with values
        chartRef.current.data.labels = messages.filter(message => message.topic === topic).map((message) => message.offset);
        chartRef.current.data.datasets[0].data = values;
        chartRef.current.update();
      } else {
        createValueGraph();
      }
    }
    if (document.getElementById('distributionCanvas')) {
      if (distributionChartRef.current) {
        const values = messages.filter(message => message.topic === topic).map(message => message.value[vizKey]);

        // Update chart with value distribution
        const distributionData = calculateValueDistribution(values);
        distributionChartRef.current.data.labels = distributionData.labels;
        distributionChartRef.current.data.datasets[0].data = distributionData.values;
        distributionChartRef.current.update();
      } else {
        createDistributionGraph();
      }
    }
    if (document.getElementById('compareCanvas')) {
      if (compareChartRef.current) {
        const distributionData = compareValues();
        compareChartRef.current.data.labels = distributionData.labels;
        compareChartRef.current.data.datasets[0].data = distributionData.values;
        compareChartRef.current.update();
      } else {
        createCompareGraph();
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
    subscribeToTopic([topic]);
    const intervalId = setInterval(async () => {
      const messageResponse = await fetchMessages();
      if (messageResponse) setMessages((prevMessages) => [...prevMessages, ...messageResponse]);
    }, 1000);

    return () => {
      clearInterval(intervalId);
    };
  }, []);

  return (
    <div>
      <h3 class="text-xl mb-4">Topic: {topic}</h3>

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

      <label class="my-3 flex items-center">
        <input type="checkbox" class="m-2" checked={progCheck} onChange={handleProgCheckChange} />
        <span>Execute charging prognose on topic</span>
      </label>

      {progResult && progCheck && (
        <>
          <h3 class="text-lg">Prognose Result</h3>
          <p>{JSON.stringify(progResult)}</p>
        </>
      )}

      <form onSubmit={handleKeySubmit} class="py-3">
        <label>Enter specific key to visualize:</label>
        <br />
        <input
          type="text"
          name="vizKey"
          defaultValue="value"
          class="p-2 rounded mr-2" />
        <br />
        <button type="submit" class="my-2">Submit</button>
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

          <h2 class="text-2xl my-4">Compare to another Topic</h2>
          {/* Input for the plaintext and annonymized topic names*/}
          <form onSubmit={handleCompareSubmit} class="pb-3">
            <label>Topic to compare:</label>
            <br />
            <input
              type="text"
              name="compareTopic"
              class="p-2 rounded mr-2"
            />
            <br />
            <label>Message UUID-key</label>
            <br />
            <input
              type="text"
              name="idKey"
              defaultValue={messageIdKey}
              class="p-2 rounded mr-2"
            />
            <br />
            <button type="submit" class="my-2">Submit</button>
          </form>

          {comparedTopic.length > 0 && (
            <>
              <Accordion>
                <AccordionSummary
                  expandIcon={<ExpandMoreIcon />}
                  aria-controls="panel3a-content"
                  id="panel3a-header"
                >
                  <Typography>Applied Noise Distribution</Typography>
                </AccordionSummary>
                <AccordionDetails>
                  <canvas id="compareCanvas" />
                </AccordionDetails>
              </Accordion>
            </>
          )}
        </>
      )}


    </div>
  );
};

export default Plt;
