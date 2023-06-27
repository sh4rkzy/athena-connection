const acessAthena = require('../config/aws.configure');

const params = {
    QueryString: 'SELECT * FROM "cdr"."tb_cdr_voz_unificado" limit 10',
    ResultConfiguration: {
        OutputLocation: process.env.AWS_S3_OUTPUT_LOCATION,
    }
};

class ExecutionController {
    async executeQuery(params) {

        try {
            const queryExecution = await acessAthena.startQueryExecution(params).promise();
            const queryExecutionId = queryExecution.QueryExecutionId;
            console.log('Query execution ID:', queryExecutionId);
            while (true) {
                const queryExecutionStatus = await acessAthena.getQueryExecution({QueryExecutionId: queryExecutionId}).promise();
                const state = queryExecutionStatus.QueryExecution.Status.State;

                if (state === 'SUCCEEDED') {
                    console.log('Query execution completed successfully.');
                    // console.log(queryExecutionStatus)
                    break;
                } else if (state === 'FAILED' || state === 'CANCELLED') {
                    console.log('Query execution failed or was cancelled.');
                    break;
                }

                await new Promise(resolve => setTimeout(resolve, 5000));
            }

            const queryResults = await acessAthena.getQueryResults({QueryExecutionId: queryExecutionId}).promise();
            const columns = queryResults.ResultSet.ResultSetMetadata.ColumnInfo.map(column => column.Name);

            // const rows = queryResults.ResultSet.Rows.map(row => row.Data.map(data => data.VarCharValue));
            // console.log('Rows:', rows);

            const rows = [];
            for (let i = 1; i < queryResults.ResultSet.Rows.length; i++) {
                const rowData = {};
                for (let j = 0; j < queryResults.ResultSet.Rows[i].Data.length; j++) {
                    const columnName = columns[j];
                    const columnData = queryResults.ResultSet.Rows[i].Data[j].VarCharValue;
                    rowData[columnName] = columnData;
                }
                rows.push(rowData);
            }

            const result = {
                columns: columns,
                rows: rows
            };

            console.log('Result:');
            console.log(JSON.stringify(result));

        } catch (error) {
            console.error('Error executing Athena query:', error);
        }
    }

}

module.exports = new ExecutionController().executeQuery(params);