import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import Chart from 'chart.js/auto';

const Plt = ({ restProxyUrl, topic }) => {
  const [messages, setMessages] = useState([]);
  const chartRef = useRef(null);
  const distributionChartRef = useRef(null);

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


  // Poll for new messages
  useEffect(() => {
    const intervalId = setInterval(fetchMessages, 1000);
    subscribeToTopic();

    return () => {
      clearInterval(intervalId);
    };
  }, []);

  // Initialize charts
  useEffect(() => {
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

    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
      if (distributionChartRef.current) {
        distributionChartRef.current.destroy();
        distributionChartRef.current = null;
      }
    };
  }, []);

  // Update charts on message change
  useEffect(() => {
    if (chartRef.current && distributionChartRef.current) {
      const values = messages.map((message) => message.value['value']);

      // Update chart with values
      chartRef.current.data.labels = messages.map((message) => message.offset);
      chartRef.current.data.datasets[0].data = values;
      chartRef.current.update();

      // Update chart with value distribution
      const distributionData = calculateValueDistribution(values);
      distributionChartRef.current.data.labels = distributionData.labels;
      distributionChartRef.current.data.datasets[0].data = distributionData.values;
      distributionChartRef.current.update();
    }
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

  return (
    <div>
      <canvas id="graphCanvas" />
      <canvas id="distributionCanvas" />
    </div>
  );
};

export default Plt;
