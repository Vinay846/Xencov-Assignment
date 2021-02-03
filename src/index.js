const express = require('express');
const { Sequelize, DataTypes } = require('sequelize');
const csv = require('csv-parser');
const fs = require('fs');
const multer = require('multer');
const path = require('path');

const app = express();
app.use(express.json());
app.use('./uploads', express.static(path.join(__dirname, '/uploads')));

const storage = multer.diskStorage({
	destination: (req, file, cb) => {
		cb(null, 'uploads');
	},
	filename: (req, file, cb) => {
		// console.log(file);
		cb(null, Date.now() + '_' + file.originalname);
	}
});

const fileFilter = (req, file, cb) => {
	if (file.mimetype == 'text/csv') {
		cb(null, true);
	} else {
		cb(null, false);
	}
};

const upload = multer({ storage: storage, fileFilter: fileFilter });

const conString = 'postgres://postgres:147258@localhost:5432/salesDatas';
const db = new Sequelize(conString, {
	dialect: 'postgres'
});

const salesData = db.define('salesDatas', {
	Id: {
		type: DataTypes.UUID,
		defaultValue: Sequelize.UUIDV4,
		primaryKey: true
	},
	'Order ID': {
		type: DataTypes.INTEGER
	},
	Region: {
		type: DataTypes.STRING
	},
	Country: {
		type: DataTypes.STRING
	},
	'Item Type': {
		type: DataTypes.STRING
	},
	'Sales Channel': {
		type: DataTypes.STRING
	},
	'Order Priority': {
		type: DataTypes.STRING
	},
	'Order Date': {
		type: DataTypes.STRING
	},
	'Ship Date': {
		type: DataTypes.STRING
	},
	'Units Sold': {
		type: DataTypes.STRING
	},
	'Unit Price': {
		type: DataTypes.STRING
	},
	'Unit Cost': {
		type: DataTypes.STRING
	},
	'Total Revenue': {
		type: DataTypes.STRING
	},
	'Total Cost': {
		type: DataTypes.STRING
	},
	'Total Profit': {
		type: DataTypes.STRING
	}
});

const addDataToDB = async (data) => {
	await salesData.bulkCreate(data);
};

app.get('/addtodb/:fileName', async (req, res) => {
	let result = [];
	const fileName = req.params.fileName;
	const stats = fs.statSync(`./uploads/${fileName}`);
	if (!stats.isFile()) {
		return res.send({ error: 'file not found !' });
	}
	const streamData = fs.createReadStream(`./uploads/${fileName}`);
	streamData
		.pipe(csv({}))
		.on('data', async (data) => {
			if (result.length > 30000) {
				addDataToDB(result);
				result = [];
			}
			result.push(data);
		})
		.on('end', () => {
			if (result.length < 30000 && result.length > 0) {
				addDataToDB(result);
				result = [];
			}
			res.send({ messge: 'successfully data added in db' });
		});
});

app.get('/:orderid', async (req, res) => {
	const orderid = req.params.orderid;
	const data = await salesData.findOne({ where: { 'Order ID': orderid } });
	if (data === null) {
		return res.send({ error: 'Not found' });
	}
	res.send({ data });
});

app.post('/upload', upload.single('file'), (req, res, next) => {
	try {
		return res.status(201).json({
			message: 'File uploaded successfully'
		});
	} catch (error) {
		console.error(error);
		res.send({ error: 'File uploaded Failed' });
	}
});

app.get('/readfiles', (req, res) => {
	let filesName = [];
	fs.readdir('./uploads', (err, files) => {
		if (err) {
			console.log('unable to read file ', err);
			res.send('error');
		} else {
			files.forEach((file) => {
				filesName.push(file);
			});
			res.send({ filesName });
		}
	});
});

app.delete('/:fileName', (req, res) => {
	const fileName = req.params.fileName;
	fs.unlink(`./uploads/${fileName}`, (err) => {
		if (err) {
			res.status(404).send('File not found');
		} else {
			res.sendStatus(200);
		}
	});
});

db
	.sync()
	.then(() => {
		app.listen(9999, () => {
			console.log('server connected !');
		});
	})
	.catch((e) => {
		console.log('some error !', e);
	});
