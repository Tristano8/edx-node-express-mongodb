const async = require('async');
const mongodb = require('mongodb');

const MongoClient = mongodb.MongoClient;

const url = 'mongodb://localhost:27017/assignment3';
const customers = require('./m3-customer-data.json')
const customerAddresses = require('./m3-customer-data.json');


let tasks = []
const limit = parseInt(process.argv[2], 10) || 1000
MongoClient.connect(url, (err, database) => {
    if (error) return process.exit(1);
    //const db = database.db('m3-customers');

    customers.forEach((customer, index, list) => {
        // Copy customer address data to matching customer objects
        customers[index] = Object.assign(customer, customerAddresses[index])

        if (index % limit == 0) {
            const start = index
            const end = (start+limit > customers.length) ? customers.length-1 : start+limit;
            tasks.push((done) => {
                console.log(`Processing ${start}-${end} out of ${customers.length}`)
                db.collection('customers'.insert(customers.slice(start, end), (error, result) =>
                    done(error, results)))
            })
        }
    })

    console.log(`Launching ${tasks.length} parralel task(s)`);
    const startTime = Date.now();
    async.parallel(tasks, (error, results) => {
    if (error) console.error(error)
    const endTime = Date.now();
    console.log(`Execution time: ${endTime-startTime}`)
});
db.close();
});