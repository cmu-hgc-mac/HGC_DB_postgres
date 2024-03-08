import sys
import numpy as np
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QLabel
from PyQt5.QtCore import Qt

class TriStateButton(QPushButton):
    def __init__(self, state_counter, state_counter_labels, parent=None):
        super().__init__(parent)
        self.dotSize = 15
        self.setFixedSize(self.dotSize, self.dotSize)
        self.state = 0  # 0: default, 1: orange with '?', 2: red with 'X'
        self.setStyleSheet(f"border-radius: {int(self.dotSize/2)}px; background-color: #3498db; color: white;")
        self.clicked.connect(self.changeState)
        self.state_counter = state_counter
        self.state_counter_labels = state_counter_labels

    def changeState(self):
        old_state = self.state
        self.state = (self.state + 1) % 3
        self.state_counter[old_state] -= 1
        self.state_counter[self.state] += 1
        self.updateCounter()
        if self.state == 0:
            self.setStyleSheet(f"border-radius: {int(self.dotSize/2)}px; background-color: #3498db; color: white;")
            self.setText("")
        elif self.state == 1:
            self.setStyleSheet(f"border-radius: {int(self.dotSize/2)}px; background-color: #e67e22; color: white;")
            self.setText("?")
        elif self.state == 2:
            self.setStyleSheet(f"border-radius: {int(self.dotSize/2)}px; background-color: #e74c3c; color: white;")
            self.setText("X")
        self.setFixedSize(self.dotSize, self.dotSize)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        radius = min(self.width(), self.height()) // 2
        self.setStyleSheet(f"border-radius: {radius}px; background-color: {self.palette().button().color().name()}; color: white;")

    def updateCounter(self):
        altlab = ['nom','redo','bad']        
        for state, count_label in self.state_counter_labels.items():
            count_label.setText(f"{altlab[state]}: {self.state_counter[state]}")

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Tri-State Buttons")
        self.setGeometry(100, 100, 600, 400)
        center_x = self.width() / 2
        center_y = self.height() / 2
        print(center_y,center_x)

        # labels = ['a', 'b', 'c', 'd', 'e']
        # positions = [[0.1, 0.3], [0.6, 0.2], [0.6, 0.8], [0.3, 0.5], [0.9, 0.5]]
        # np.random.seed(10); n = 10; 
        # positions = np.array([np.random.rand(n),np.random.rand(n)]).transpose()
        # labels = np.arange(50).astype(str)
        # self.state_counter = {0: n, 1: 0, 2: 0}

        s = 6
        positions = np.array([[r*np.cos(2*np.pi*i/s),r*np.sin(2*np.pi*i/s)] for i in range(s) for r in [0.15, 0.2,0.35,0.43]])
        labels = np.arange(len(positions)).astype(str)

        self.state_counter = {0: len(positions), 1: 0, 2: 0}
        self.state_counter_labels = {}

        altlab = ['nom','redo','bad']
        for state in self.state_counter:
            lab = QLabel(f"{altlab[state]}: {self.state_counter[state]}", self)
            lab.move(20, 20 + state * 30)
            self.state_counter_labels[state] = lab

        for pos, label_text in zip(positions, labels):
            button = TriStateButton(self.state_counter, self.state_counter_labels, self)
            x = int(pos[0] * self.width() + center_x - button.width()/2)
            y = int(pos[1] * self.height() + center_y - button.height()/2)
            button.move(x, y)

             # Create label for the button
            label = QLabel(label_text, self)
            label.move(x + button.width() + 5, y + button.height())
            label.setAlignment(Qt.AlignVCenter)
            label.setStyleSheet("font-size: 16px;")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    mainWindow = MainWindow()
    mainWindow.show()
    sys.exit(app.exec_())

