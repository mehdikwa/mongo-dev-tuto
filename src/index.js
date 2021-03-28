const MongoClient = require("mongodb").MongoClient;

const source = {
  url: "mongodb://localhost:27017",
  db: "insee",
  collection: "departements"
};

const target = {
  url: "mongodb://localhost:27017",
  db: "insee",
  collection: "newDepartements"
};


const loadData = async datasource => {
  const client = await MongoClient.connect(datasource.url, {
    useNewUrlParser: true
  });

  try {
    const collection = client
      .db(datasource.db)
      .collection(datasource.collection);

    const aggregate = [
      {
        $match: {
          social_fiscal_2013: { $type: "object" }
        }
      },
      {
        $project: {
          _id: 1,
          NCCENR: 1,
          nombre_de_menages: "$social_fiscal_2013.menages_fiscaux.nombre_menages",
        }
      }
    ];

    const cursor = await collection.aggregate(aggregate);
    return await timedOperation("> Time spent reading data", () => cursor.toArray());
  } finally {
    if (client) {
      client.close();
    }
  }
};

const writeData = async (datasource, docs) => {
  const client = await MongoClient.connect(datasource.url, {
    useNewUrlParser: true
  });

  try {
    const collection = client
      .db(datasource.db)
      .collection(datasource.collection);

    const bulkOperation = collection.initializeUnorderedBulkOp();

    docs.forEach(doc => {
      bulkOperation
        .find({
          _id: doc._id
        })
        .upsert()
        .replaceOne({
          _id: doc._id,
          social_fiscal_2013: doc.nombre_de_menages
        });
    });

    try {
      await timedOperation("> Time spent writing data", () => bulkOperation.execute());
    } catch (e) {
      console.error(
        `An error occurred while processing bulk upsert of documents ${docs
          .map(p => p._id)
          .join()}`,
        e
      );
    }
  } finally {
    if (client) {
      client.close();
    }
  }
};

const timedOperation = async (msg, fn) => {
  // tslint:disable-next-line:no-console
  console.time(msg);
  const result = await fn();
  // tslint:disable-next-line:no-console
  console.timeEnd(msg);
  return result;
};

const main = async () => {
  console.log("Make sure to run this script with more memory, eg. `node --max-old-space-size=4096 script.js`");

  const docs = await loadData(source);
  if (docs.length > 0) {
    await writeData(target, docs);
  } else {
    console.error(`No documents in source database, cannot export them. Source db: ${source.db}/${source.collection}`);
  }
};

main()
  .then(() => {
    process.exit(0);
  })
  .catch(e => {
    console.error("An error occurred", e);
    process.exit(1);
  });
