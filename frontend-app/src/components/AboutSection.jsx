export default function AboutSection() {
  return (
    <div className="about-section">
      <h2>About STL Metro Data API</h2>
      
      <div className="info-card">
        <h3>What is this project?</h3>
        <p>
          The purpose of our project is to unify data about the City of St. Louis 
          from a variety of sources and have a single, easy place for people to 
          access this data. That way we all can make better informed decisions.
        </p>
      </div>

      <div className="info-card">
        <h3>Where is the data sourced from?</h3>
        <p>
          Currently, our data is sourced from{' '}
          <a href="https://data.stlouis-mo.gov/">
            The City of St. Louis Open Data
          </a> website.
        </p>
      </div>

      <div className="info-card">
        <h3>How do developers use this project?</h3>
        <p>
          Access our API endpoints at{' '}
          <a href="http://localhost:5003/swagger">
            http://localhost:5003/swagger
          </a>
        </p>
      </div>
    </div>
  );
}