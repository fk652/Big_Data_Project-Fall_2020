import '../css/main.css';
import logo from '../logo.svg';

function Footer() {
  return (
    <div className="footer-container">
        <img src={logo} className="react-logo" alt="logo" />
        <p><code>Built with React</code></p>
    </div>
  );
}

export default Footer;