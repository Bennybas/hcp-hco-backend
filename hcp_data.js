const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors'); // Add this line
const app = express();
const PORT = 5000;

// Enable CORS for all routes
app.use(cors()); // Add this line

// MongoDB Connection
mongoose.connect('mongodb://localhost:27017/zolgensma', { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('Connected to MongoDB'))
  .catch((err) => console.log('Error connecting to MongoDB: ', err));

// Define Schema for Master Data
const ReferalSchema = new mongoose.Schema({}, { strict: false });
const ReferalData = mongoose.model('ReferalData', ReferalSchema, 'referal');

// API route to fetch HCP data
const hcpSchema = new mongoose.Schema({}, { strict: false });
const HcpData = mongoose.model('HcpData', hcpSchema, 'hcp'); 

// Add some logging to debug API calls
app.get('/fetch-data', async (req, res) => {
  console.log('Received request to /fetch-data');
  try {
    const data = await HcpData.find(); 
    // console.log(`Found ${data.length} HCP records`);
    res.json(data); 
  } catch (err) {
    console.error('Error fetching HCP data:', err);
    res.status(500).json({ message: 'Error fetching HCP data', error: err.message });
  }
});

// API route to fetch Master Data
app.get('/fetch-referal-data', async (req, res) => {
  console.log('Received request to /fetch-referal-data');
  try {
    const refer_data = await ReferalData.find();
    // console.log(`Found ${refer_data.length} referral records`);
    res.json(refer_data);
  } catch (err) {
    console.error('Error fetching Master Data:', err);
    res.status(500).json({ message: 'Error fetching Master Data', error: err.message });
  }
});

// Add a simple test route
app.get('/test', (req, res) => {
  res.json({ message: 'API is working!' });
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  console.log('CORS is enabled for all origins');
});