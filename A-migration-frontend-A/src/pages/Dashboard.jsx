import React, { useState } from "react";
import {
  Grid,
  Column,
  Search,
  Select,
  SelectItem,
  DataTable,
  Table,
  TableHead,
  TableRow,
  TableHeader,
  TableBody,
  TableCell,
  TableContainer,
  Button,
} from "@carbon/react";
import {
  ArrowDownRight,
  ArrowUpRight,
  ChartBar,
} from "@carbon/icons-react";
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
} from "recharts";
import Header from "../components/Header";
import Footer from "../components/Footer";
import "../styles/Dashboard.css";

// Import the new component
import ViewDDL from "../components/ViewDDL";

const Dashboard = () => {
  const [selectedMonth, setSelectedMonth] = useState("July");

  // State for ViewDDL modal open
  const [isViewDdlOpen, setIsViewDdlOpen] = useState(false);

  const handleMonthChange = (e) => {
    setSelectedMonth(e.target.value);
  };

  // Dummy static data for charts
  const migrationData = [
    { date: "Jul 01", count: 3 },
    { date: "Jul 08", count: 8 },
    { date: "Jul 15", count: 4 },
    { date: "Jul 22", count: 9 },
    { date: "Jul 25", count: 11 },
  ];

  const migrationRecords = [
    {
      id: "TXN001",
      schema: "MAXIMO",
      type: "Tables",
      timestamp: "2025-07-24 12:34",
      status: "Success",
      reason: "-",
    },
    {
      id: "TXN002",
      schema: "MAXIMO",
      type: "Views",
      timestamp: "2025-07-24 14:50",
      status: "Failed",
      reason: "Connection Timeout",
    },
    {
      id: "TXN003",
      schema: "MAXIMO",
      type: "Sequences",
      timestamp: "2025-07-24 15:00",
      status: "Success",
      reason: "-",
    },
  ];

  return (
    <>
      <Header />
      <Grid fullWidth className="dashboard-container">
        <Column sm={4} md={8} lg={16}>
          <h1 className="dashboard-title">
            Welcome to the <span className="highlight-blue">Conversion Dashboard</span>
          </h1>
          <p className="dashboard-subtitle">
            Track and visualize your Oracle/SQL to Db2 database conversions with insights.
          </p>
        </Column>

        {/* Summary Cards */}
        <Column sm={4} md={4} lg={5}>
          <div className="card total">
            <ChartBar size={48} className="card-icon" />
            <div>
              <p>Total Conversions</p>
              <h3>30</h3>
            </div>
          </div>
        </Column>
        <Column sm={4} md={4} lg={5}>
          <div className="card success">
            <ArrowUpRight size={48} className="card-icon" />
            <div>
              <p>Successful Conversions</p>
              <h3>24</h3>
            </div>
          </div>
        </Column>
        <Column sm={4} md={4} lg={5}>
          <div className="card failure">
            <ArrowDownRight size={48} className="card-icon" />
            <div>
              <p>Failed Conversions</p>
              <h3>6</h3>
            </div>
          </div>
        </Column>

        {/* Migration Trend */}
        <Column sm={4} md={8} lg={16} className="chart-container">
          <br />
          <br />
          <div className="chart-header">
            <h3>Conversion Trend</h3>
            <Select
              id="month-select"
              labelText=""
              defaultValue="July"
              onChange={handleMonthChange}
              className="month-select"
            >
              <SelectItem value="July" text="July" />
              <SelectItem value="June" text="June" />
              <SelectItem value="May" text="May" />
            </Select>
          </div>

          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={migrationData}>
              <defs>
                <linearGradient id="colorCount" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#0f62fe" stopOpacity={0.8} />
                  <stop offset="95%" stopColor="#0f62fe" stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis dataKey="date" />
              <YAxis />
              <CartesianGrid strokeDasharray="3 3" />
              <Tooltip />
              <Area type="monotone" dataKey="count" stroke="#0f62fe" fillOpacity={1} fill="url(#colorCount)" />
            </AreaChart>
          </ResponsiveContainer>
        </Column>

        {/* Recent Migrations */}
        <Column sm={4} md={8} lg={16}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <h4 className="section-title">Recent Conversions</h4>
            <Button size="sm" onClick={() => setIsViewDdlOpen(true)}>
              View DDL
            </Button>
          </div>

          <Search labelText="Search by schema or transaction ID" placeholder="Search..." className="search-bar" />

          <DataTable
            rows={migrationRecords}
            headers={[
              { key: "id", header: "Transaction ID" },
              { key: "schema", header: "Schema" },
              { key: "type", header: "Type" },
              { key: "status", header: "Status" },
              { key: "timestamp", header: "Timestamp" },
              { key: "reason", header: "Failure Reason" },
            ]}
          >
            {({ rows, headers, getTableProps, getHeaderProps, getRowProps }) => (
              <TableContainer>
                <Table {...getTableProps()}>
                  <TableHead>
                    <TableRow>
                      {headers.map((header) => (
                        <TableHeader {...getHeaderProps({ header })} key={header.key}>
                          {header.header}
                        </TableHeader>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {rows.map((row) => (
                      <TableRow {...getRowProps({ row })} key={row.id}>
                        {row.cells.map((cell) => (
                          <TableCell key={cell.id}>{cell.value || "—"}</TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </DataTable>
        </Column>
      </Grid>

      {/* ViewDDL Modal */}
      <ViewDDL open={isViewDdlOpen} onClose={() => setIsViewDdlOpen(false)} />

      <Footer />
    </>
  );
};

export default Dashboard;
