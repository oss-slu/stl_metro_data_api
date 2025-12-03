import { useState, useEffect } from 'react';

/*const fetchCSBData = async () => { 
  const response = await fetch('http://localhost:5003/csb');
  if (!response.ok) throw new Error('Failed to fetch CSB data');
  return response.json();
};*/

//these two functions are only for while there is no database connection, purely for viewing purposes, to be replaced with above once databse is connected
const fetchCSBData = async () => {
  try {
    const response = await fetch('http://localhost:5003/csb');
    if (!response.ok) throw new Error('Failed to fetch CSB data');
    return response.json();
  } catch (error) {
    // Return mock data if API fails
    console.log('API unavailable, using mock data');
    return getMockData();
  }
};

// Mock data function
function getMockData() {
  return [
    {
      id: 1,
      service_name: "Sidewalk Repair",
      description: "Sidewalk is cracked and uneven, creating a tripping hazard",
      is_active: 1,
      contact_info: {
        caller_type: "Resident",
        neighborhood: "Central West End",
        ward: "17",
      },
      data_posted_on: "2025-03-15T10:30:00",
      source_url: "https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5"
    },
    {
      id: 2,
      service_name: "Inspect City Tree",
      description: "Tree appears diseased with dead branches",
      is_active: 1,
      contact_info: {
        caller_type: "Phone",
        neighborhood: "Tower Grove South",
        ward: "08",
      },
      data_posted_on: "2025-03-14T14:20:00",
      source_url: "https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5"
    },
    {
      id: 3,
      service_name: "Street Pothole Repair",
      description: "Large pothole causing vehicle damage",
      is_active: 1,
      contact_info: {
        caller_type: "Web",
        neighborhood: "The Hill",
        ward: "09",
      },
      data_posted_on: "2025-03-13T09:15:00",
      source_url: "https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5"
    },
    {
      id: 4,
      service_name: "Refuse Collection-Missed Pickup",
      description: "Trash was not collected on scheduled day",
      is_active: 1,
      contact_info: {
        caller_type: "Resident",
        neighborhood: "Downtown",
        ward: "07",
      },
      data_posted_on: "2025-03-12T08:45:00",
      source_url: "https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5"
    },
    {
      id: 5,
      service_name: "Sewer Lateral Defect",
      description: "Sewer backup in basement",
      is_active: 1,
      contact_info: {
        caller_type: "Phone",
        neighborhood: "Soulard",
        ward: "06",
      },
      data_posted_on: "2025-03-11T16:30:00",
      source_url: "https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5"
    }
  ];
}

function CSBDashboard() {
  const [requests, setRequests] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedWard, setSelectedWard] = useState('all');
  const [selectedService, setSelectedService] = useState('all');

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    try {
      setLoading(true);
      const data = await fetchCSBData();
      setRequests(data);
      setError(null);
    } catch (err) {
      setError(err.message);
      setRequests([]);
    } finally {
      setLoading(false);
    }
  };

  const wards = ['all', ...new Set(requests.map(r => r.contact_info?.ward).filter(Boolean))].sort();
  const services = ['all', ...new Set(requests.map(r => r.service_name).filter(Boolean))].sort();

  const filteredRequests = requests.filter(request => {
    const matchesSearch = !searchTerm || 
      request.service_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      request.description?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      request.contact_info?.neighborhood?.toLowerCase().includes(searchTerm.toLowerCase());
    
    const matchesWard = selectedWard === 'all' || request.contact_info?.ward === selectedWard;
    const matchesService = selectedService === 'all' || request.service_name === selectedService;
    
    return matchesSearch && matchesWard && matchesService;
  });

  const stats = {
    total: filteredRequests.length,
    services: new Set(filteredRequests.map(r => r.service_name)).size,
    wards: new Set(filteredRequests.map(r => r.contact_info?.ward)).size
  };

  if (loading) {
    return (
      <div style={styles.loadingContainer}>
        <div style={styles.spinner}></div>
        <p>Loading CSB 311 data...</p>
      </div>
    );
  }

  return (
    <div style={styles.container}>
      {/* Header */}
      <header style={styles.header}>
        <h1 style={styles.title}>STL CSB 311 Service Requests</h1>
        <p style={styles.subtitle}>Citizens' Service Bureau - Active Requests Dashboard</p>
        {error && (
          <div style={styles.errorBanner}>
            ⚠️ Could not connect to API - Please ensure backend is running on port 5003
          </div>
        )}
      </header>

      {/* Stats */}
      <div style={styles.statsContainer}>
        <div style={styles.statCard}>
          <h3 style={styles.statLabel}>Total Active</h3>
          <p style={styles.statValue}>{stats.total}</p>
        </div>
        <div style={styles.statCard}>
          <h3 style={styles.statLabel}>Service Types</h3>
          <p style={styles.statValue}>{stats.services}</p>
        </div>
        <div style={styles.statCard}>
          <h3 style={styles.statLabel}>Wards</h3>
          <p style={styles.statValue}>{stats.wards}</p>
        </div>
      </div>

      {/* Filters */}
      <div style={styles.filtersContainer}>
        <input
          type="text"
          placeholder="Search requests..."
          style={styles.searchInput}
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
        />
        
        <select
          style={styles.select}
          value={selectedWard}
          onChange={(e) => setSelectedWard(e.target.value)}
        >
          <option value="all">All Wards</option>
          {wards.filter(w => w !== 'all').map(ward => (
            <option key={ward} value={ward}>Ward {ward}</option>
          ))}
        </select>

        <select
          style={styles.select}
          value={selectedService}
          onChange={(e) => setSelectedService(e.target.value)}
        >
          <option value="all">All Services</option>
          {services.filter(s => s !== 'all').map(service => (
            <option key={service} value={service}>{service}</option>
          ))}
        </select>
      </div>

      {/* Results Count */}
      <div style={styles.resultsCount}>
        Showing {filteredRequests.length} active service requests
      </div>

      {/* Requests List */}
      <div style={styles.requestsContainer}>
        {filteredRequests.length === 0 ? (
          <div style={styles.noResults}>
            <p>No requests found matching your filters</p>
          </div>
        ) : (
          filteredRequests.map((request) => (
            <RequestCard key={request.id} request={request} />
          ))
        )}
      </div>

      {/* Footer */}
      <footer style={styles.footer}>
        <p>{new Date().getFullYear()} SLU Open Source</p>
        <p style={styles.footerLink}>
          Data source: <a href="https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5" target="_blank" rel="noopener noreferrer">
            St. Louis Open Data
          </a>
        </p>
      </footer>
    </div>
  );
}

function RequestCard({ request }) {
  const formatDate = (dateStr) => {
    if (!dateStr) return 'N/A';
    return new Date(dateStr).toLocaleDateString('en-US', { 
      month: 'short', 
      day: 'numeric',
      year: 'numeric'
    });
  };

  return (
    <div style={styles.requestCard}>
      <div style={styles.requestHeader}>
        <h3 style={styles.requestTitle}>{request.service_name || 'Unknown Service'}</h3>
        <span style={styles.activeBadge}>Active</span>
      </div>
      
      {request.description && (
        <p style={styles.requestDescription}>{request.description}</p>
      )}
      
      <div style={styles.requestDetails}>
        {request.contact_info?.ward && (
          <span style={styles.detailItem}>📍 Ward {request.contact_info.ward}</span>
        )}
        {request.contact_info?.neighborhood && (
          <span style={styles.detailItem}>🏘️ {request.contact_info.neighborhood}</span>
        )}
        {request.data_posted_on && (
          <span style={styles.detailItem}>📅 {formatDate(request.data_posted_on)}</span>
        )}
        {request.contact_info?.caller_type && (
          <span style={styles.detailItem}>👤 {request.contact_info.caller_type}</span>
        )}
      </div>
      
      {request.source_url && (
        <a 
          href={request.source_url} 
          target="_blank" 
          rel="noopener noreferrer"
          style={styles.sourceLink}
        >
          View source data →
        </a>
      )}
    </div>
  );
}

const styles = {
  container: {
    fontFamily: 'system-ui, -apple-system, sans-serif',
    backgroundColor: '#f5f5f5',
    minHeight: '100vh',
    margin: 0,
    width: '100%',
  },
  loadingContainer: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    height: '100vh',
    backgroundColor: '#f5f5f5',
  },
  spinner: {
    border: '4px solid #f3f3f3',
    borderTop: '4px solid #2563eb',
    borderRadius: '50%',
    width: '40px',
    height: '40px',
    animation: 'spin 1s linear infinite',
  },
  header: {
    backgroundColor: '#2563eb',
    color: 'white',
    padding: '2rem 1rem',
    textAlign: 'center',
  },
  title: {
    margin: '0 0 0.5rem 0',
    fontSize: '2rem',
  },
  subtitle: {
    margin: 0,
    opacity: 0.9,
    fontSize: '1rem',
  },
  errorBanner: {
    backgroundColor: '#fbbf24',
    color: '#78350f',
    padding: '1rem',
    marginTop: '1rem',
    borderRadius: '8px',
    maxWidth: '800px',
    marginLeft: 'auto',
    marginRight: 'auto',
  },
  statsContainer: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
    gap: '1.5rem',
    maxWidth: '100%',
    margin: '-2rem 2rem 2rem 2rem',
    padding: '0 1rem',
  },
  statCard: {
    backgroundColor: 'white',
    padding: '1.5rem',
    borderRadius: '8px',
    boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
    textAlign: 'center',
  },
  statLabel: {
    margin: '0 0 0.5rem 0',
    color: '#6b7280',
    fontSize: '0.875rem',
    fontWeight: '500',
    textTransform: 'uppercase',
  },
  statValue: {
    margin: 0,
    fontSize: '2.5rem',
    fontWeight: 'bold',
    color: '#1f2937',
  },
  filtersContainer: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
    gap: '1rem',
    maxWidth: '100%',
    margin: '0 2rem 1.5rem 2rem',
    padding: '0 1rem',
  },
  searchInput: {
    padding: '0.75rem',
    fontSize: '1rem',
    border: '1px solid #d1d5db',
    borderRadius: '8px',
    width: '100%',
    boxSizing: 'border-box',
    backgroundColor: 'white',
    color: '#1f2937',
  },
  select: {
    padding: '0.75rem',
    fontSize: '1rem',
    border: '1px solid #d1d5db',
    borderRadius: '8px',
    backgroundColor: 'white',
    color: '#1f2937',
    width: '100%',
    boxSizing: 'border-box',
  },
  resultsCount: {
    maxWidth: '100%',
    margin: '0 2rem 1rem 2rem',
    padding: '0 1rem',
    color: '#6b7280',
    fontSize: '0.875rem',
    fontWeight: '500',
  },
  requestsContainer: {
    maxWidth: '100%',
    margin: '0 2rem',
    padding: '0 1rem 2rem 1rem',
    display: 'grid',
    gap: '1rem',
  },
  noResults: {
    backgroundColor: 'white',
    padding: '3rem',
    textAlign: 'center',
    borderRadius: '8px',
    color: '#6b7280',
  },
  requestCard: {
    backgroundColor: 'white',
    padding: '1.5rem',
    borderRadius: '8px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    transition: 'box-shadow 0.2s',
    cursor: 'pointer',
  },
  requestHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'start',
    marginBottom: '0.75rem',
  },
  requestTitle: {
    margin: 0,
    fontSize: '1.25rem',
    color: '#1f2937',
  },
  activeBadge: {
    backgroundColor: '#10b981',
    color: 'white',
    padding: '0.25rem 0.75rem',
    borderRadius: '9999px',
    fontSize: '0.75rem',
    fontWeight: '600',
  },
  requestDescription: {
    color: '#6b7280',
    marginBottom: '1rem',
    lineHeight: '1.5',
  },
  requestDetails: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: '1rem',
    marginBottom: '0.75rem',
  },
  detailItem: {
    fontSize: '0.875rem',
    color: '#6b7280',
  },
  sourceLink: {
    color: '#2563eb',
    textDecoration: 'none',
    fontSize: '0.875rem',
    fontWeight: '500',
  },
  footer: {
    backgroundColor: '#1f2937',
    color: 'white',
    padding: '2rem',
    textAlign: 'center',
    marginTop: '2rem',
  },
  footerLink: {
    fontSize: '0.875rem',
    opacity: 0.8,
  },
};

export default CSBDashboard;