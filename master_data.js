const express = require('express');
const mongoose = require('mongoose');
const app = express();
const PORT = 5001;

// MongoDB Connection
mongoose.connect('mongodb://localhost:27017/zolgensma', { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('Connected to MongoDB'))
  .catch((err) => console.log('Error connecting to MongoDB: ', err));

// Define Schema for Master Data
const masterSchema = new mongoose.Schema({}, { strict: false });
const MasterData = mongoose.model('MasterData', masterSchema, 'master'); // Ensure the collection name is 'master'

// API route to fetch Master Data
app.get('/fetch-master-data', async (req, res) => {
  try {
    const masterData = await MasterData.find(); 
    if (masterData.length === 0) {
      return res.status(404).json({ message: "No Master Data Found" });
    }
    res.json(masterData);
  } catch (err) {
    res.status(500).json({ message: 'Error fetching Master Data', error: err });
  }
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
