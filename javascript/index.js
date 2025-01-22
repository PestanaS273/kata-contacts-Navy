const sqlite3 = require('sqlite3')
const open = require('sqlite').open
const fs = require('fs')
const process = require('process')
const { create } = require('domain')

const filename = 'contacts.sqlite3'
const numContacts = parseInt(process.argv[2], 10);

const shouldMigrate = !fs.existsSync(filename)

const migrate = async (db) => {
  console.log('Migrating db ...')
  await db.exec(`
        CREATE TABLE contacts(
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT NOT NULL
         )
     `)
  console.log('Done migrating db')
}

const createIndex = async (db) => {
  console.log('Creating index ...')
  await db.exec('CREATE INDEX email_index ON contacts (email)')
  console.log('Done creating index')
}

const insertManyContacts = async (db, numContacts) => {
  console.log(`Inserting ${numContacts} contacts ...`)

  await db.run('BEGIN TRANSACTION')
  const stmt = await db.prepare('INSERT INTO contacts (name, email) VALUES (?, ?)')

  try {
    for (let i = 1; i <= numContacts; i++) {
      await stmt.run(`name-${i}`, `email-${i}@domain.tld`)
      // Commit every 1000 records to avoid memory issues
      if (i % 1000 === 0) {
        await db.run('COMMIT')
        await db.run('BEGIN TRANSACTION')
      }
    }
    await stmt.finalize()
    await db.run('COMMIT')
  } catch (err) {
    await db.run('ROLLBACK')
    throw err
  }


  console.log('Done inserting contacts')
}

const queryContact = async (db) => {
  return await db.get('SELECT name FROM contacts WHERE email = ?', [`email-${numContacts}@domain.tld`])
}


(async () => {
  const db = await open({
    filename,
    driver: sqlite3.Database
  })

  if (shouldMigrate) {
    await migrate(db)
    await createIndex(db)
  }
  await insertManyContacts(db, numContacts)

  const start = Date.now()
  const contact = await queryContact(db)
  const end = Date.now()
  const elapsed = end - start
  console.log(`Query took ${elapsed} milliseconds`)

  if (!contact || !contact.name) {
    console.error('Contact not found')
    process.exit(1)
  }

  await db.close()
})()
